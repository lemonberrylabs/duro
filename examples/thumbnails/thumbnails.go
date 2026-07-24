// Package main demonstrates duro's parallelism toolkit on a thumbnail
// rendering fleet: declared queues with concurrency/rate/priority/partition
// controls, FanOut child options (idempotent IDs, deduplication, timeouts,
// delays, auth), a hand-written child workflow using duro.Context, in-process
// bounded Parallel, and a registered pipeline used directly as a FanOut child.
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lemonberrylabs/duro"
)

// --- domain types ------------------------------------------------------------

type Image struct {
	ID          string
	Region      string // routes delivery onto a queue partition
	ContentHash string // deduplicates identical uploads
}

type Thumb struct {
	ImageID string
	Size    int
	Bytes   int
}

type Rendered struct {
	ImageID string
	Thumbs  []Thumb
}

type Delivered struct {
	ImageID string
	Region  string
	Count   int
}

type Manifest struct {
	Delivered []Delivered
}

// --- queues ------------------------------------------------------------------
// Declared once, referenced by value. duro.Register auto-registers every
// queue a pipeline touches, so there is no separate registration call to
// keep in sync.

var (
	// renderQueue bounds the fleet: at most 3 renders at a time across all
	// executors, at most 20 render starts per second, priority-scheduled.
	renderQueue = duro.NewQueue("render",
		duro.WithConcurrency(3),
		duro.WithRateLimit(20, time.Second),
		duro.WithPriorities(),
	)
	// deliverQueue is partitioned: each region gets its own concurrency
	// limits, and this executor works at most 2 deliveries at a time.
	deliverQueue = duro.NewQueue("deliver",
		duro.WithPartitions(),
		duro.WithWorkerConcurrency(2),
	)
)

// renderRuns counts actual render executions — deduplicated uploads reuse a
// sibling's render, so this stays below the number of images.
var renderRuns atomic.Int64

// --- the render child: a hand-written workflow -------------------------------
// Hand-written workflows are declared against duro.Context and registered
// with duro.RegisterWorkflow — see main.go. Inside, RunAll executes a
// pipeline and returns every emitted value, and Parallel renders the three
// sizes concurrently in-process, bounded to 2 at a time, each size a
// checkpointed step.

var renderSizes = duro.Pipe2(
	duro.Expand("sizes", func(_ context.Context, img Image) ([]int, error) {
		renderRuns.Add(1)
		if img.ContentHash == "poison" {
			return nil, fmt.Errorf("image %s is corrupt", img.ID)
		}
		if strings.HasPrefix(img.ContentHash, "slow") {
			time.Sleep(3 * time.Second) // the strict act's "expensive model call"
		}
		return []int{64, 256, 1024}, nil
	}),
	duro.Parallel("render-size", 2, func(_ context.Context, size int) (Thumb, error) {
		time.Sleep(30 * time.Millisecond) // the "render"
		return Thumb{Size: size, Bytes: size * size / 3}, nil
	}),
)

// RenderImage is the FanOut child for the render stage: one durable workflow
// per image, itself running a pipeline.
func RenderImage(ctx duro.Context, img Image) (Rendered, error) {
	thumbs, err := duro.RunAll(ctx, img, renderSizes)
	if err != nil {
		return Rendered{}, err
	}
	for i := range thumbs {
		thumbs[i].ImageID = img.ID
	}
	return Rendered{ImageID: img.ID, Thumbs: thumbs}, nil
}

// --- the delivery child: a registered pipeline -------------------------------
// A *PipelineWorkflow is itself a valid FanOut child — no adapter needed;
// the reference carries its own dispatch. Registered in main.go.

var DeliverPipeline = duro.Pipe2(
	duro.Step("upload", func(_ context.Context, r Rendered) (Delivered, error) {
		time.Sleep(20 * time.Millisecond) // the "CDN upload"
		return Delivered{ImageID: r.ImageID, Count: len(r.Thumbs)}, nil
	}),
	duro.Tap("notify", func(_ context.Context, d Delivered) error {
		fmt.Printf("      [deliver] %s: %d thumbs live\n", d.ImageID, d.Count)
		return nil
	}),
)

// --- the gallery pipeline ----------------------------------------------------

// GalleryPipeline fans a batch of images out twice: renders on the bounded
// render queue (hand-written child), then deliveries on the partitioned
// queue (pipeline child), then folds a manifest.
//
// Child options shown here:
//   - WithChildID: derive the child's workflow ID from the item, making
//     renders idempotent across gallery runs — re-running the gallery
//     re-attaches to finished renders instead of rendering again.
//   - WithChildDeduplicationID + DeduplicationReturnExisting: identical
//     uploads (same content hash) collapse onto one render; both items get
//     its result.
//   - WithChildPriority: this batch renders at priority 1 on the
//     priority-enabled queue (lower runs first).
//   - WithChildTimeout: a stuck render is durably cancelled after 30s and
//     fails the pipeline.
//   - WithChildAuthenticatedUser: children carry auth metadata in their
//     status records.
//   - WithChildPartitionKey: deliveries route to per-region partitions.
//   - WithChildDelay: deliveries start DELAYED for a beat — visible in
//     `dbos.ListWorkflows` while pending.
//
// batchTag scopes child workflow IDs to this process invocation so the demo
// is repeatable against a used database. In production you would omit it —
// stable IDs like "render-"+img.ID are exactly what makes renders idempotent
// across deploys and re-runs.
func galleryPipeline(render *duro.RegisteredWorkflow[Image, Rendered], rendered *duro.PipelineWorkflow[Rendered, Delivered], batchTag string) duro.Pipeline[[]Image, Manifest] {
	return duro.Pipe4(
		duro.Expand("explode", func(_ context.Context, imgs []Image) ([]Image, error) {
			return imgs, nil
		}),
		duro.FanOut("render", renderQueue, render,
			duro.WithChildID(func(img Image) string { return "render-" + batchTag + "-" + img.ID }),
			duro.WithChildDeduplicationID(func(img Image) string { return img.ContentHash }),
			duro.WithChildDeduplicationPolicy(duro.DeduplicationReturnExisting),
			duro.WithChildPriority(1),
			duro.WithChildTimeout(30*time.Second),
			duro.WithChildAuthenticatedUser("gallery-svc"),
		),
		duro.FanOut("deliver", deliverQueue, rendered,
			duro.WithChildPartitionKey(func(r Rendered) string { return regionOf(r.ImageID) }),
			duro.WithChildDelay(300*time.Millisecond),
		),
		duro.Reduce("manifest", func(_ context.Context, acc Manifest, d Delivered) (Manifest, error) {
			d.Region = regionOf(d.ImageID)
			acc.Delivered = append(acc.Delivered, d)
			return acc, nil
		}, Manifest{}),
	)
}

// --- the strict gallery: cancel-on-failure -----------------------------------

// strictGalleryPipeline is the cost-conscious variant: renders are wasted
// spend once the batch has failed, so the fan-out opts into
// WithCancelSiblings — the first failed render cancels every sibling that is
// not yet terminal, running and never-dequeued alike, instead of draining
// them. The Rescue around it sees the failing child's error (never a
// sibling's CANCELLED result) and settles the batch with a refund.
//
// Contrast with galleryPipeline above, which keeps the drain default: its
// deliveries are cheap and every completion has value on its own. The two
// behaviors are per-stage choices, not application-wide ones.
func strictGalleryPipeline(render *duro.RegisteredWorkflow[Image, Rendered], batchTag string) duro.Pipeline[[]Image, Manifest] {
	fleet := duro.Pipe3(
		duro.Expand("explode", func(_ context.Context, imgs []Image) ([]Image, error) {
			return imgs, nil
		}),
		duro.FanOut("render", renderQueue, render,
			duro.WithChildID(func(img Image) string { return strictChildID(batchTag, img.ID) }),
			duro.WithCancelSiblings(),
		),
		duro.Reduce("manifest", func(_ context.Context, acc Manifest, r Rendered) (Manifest, error) {
			acc.Delivered = append(acc.Delivered, Delivered{ImageID: r.ImageID, Count: len(r.Thumbs)})
			return acc, nil
		}, Manifest{}),
	)
	return duro.Pipe1(
		duro.Rescue("refund", fleet, func(_ context.Context, _ []Image, cause error) (Manifest, error) {
			fmt.Printf("      [refund] batch failed, refunding the customer: %v\n", cause)
			return Manifest{}, nil
		}),
	)
}

func strictChildID(batchTag, imageID string) string {
	return "strict-" + batchTag + "-" + imageID
}

// regionOf resolves an image's region for partition routing; a real system
// would look this up, the demo records it when the batch is built.
func regionOf(imageID string) string {
	if region, ok := imageRegions.Load(imageID); ok {
		return region.(string)
	}
	return "unknown"
}

var imageRegions sync.Map // image ID → region, filled in main
