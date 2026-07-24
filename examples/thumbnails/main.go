package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"time"

	"github.com/lemonberrylabs/duro"
)

func main() {
	app, err := duro.New(context.Background(), duro.Config{
		Name:        "duro-thumbnails",
		DatabaseURL: databaseURL(),
		Logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		fatal("initializing: %v", err)
	}

	// The hand-written workflow registers like everything else — through
	// duro. The whole example is dbos-free.
	render := duro.RegisterWorkflow(app, "RenderImage", RenderImage)

	// Registered pipelines: the delivery child, then the gallery that fans
	// out onto it. Register auto-registers renderQueue and deliverQueue
	// because the pipelines reference them.
	batchTag := time.Now().Format("150405.000") // scope demo identities to this invocation
	deliver := duro.Register(app, "deliver", DeliverPipeline)
	gallery := duro.Register(app, "gallery", galleryPipeline(render, deliver, batchTag))
	strict := duro.Register(app, "gallery-strict", strictGalleryPipeline(render, batchTag))

	if err := app.Launch(); err != nil {
		fatal("launching: %v", err)
	}
	defer app.Shutdown(5 * time.Second)

	batch := []Image{
		{ID: "img-1", Region: "us", ContentHash: "aaa"},
		{ID: "img-2", Region: "eu", ContentHash: "bbb"},
		{ID: "img-3", Region: "us", ContentHash: "aaa"}, // duplicate upload of img-1
		{ID: "img-4", Region: "eu", ContentHash: "ccc"},
	}
	for _, img := range batch {
		imageRegions.Store(img.ID, img.Region)
	}

	section("gallery run (fan-out fleet: 4 images, 1 duplicate)")
	manifest := runGallery(app, gallery, batch, "gallery-"+batchTag)
	fmt.Printf("   manifest: %d delivered\n", len(manifest.Delivered))
	for _, d := range manifest.Delivered {
		fmt.Printf("     %s → region %s (%d thumbs)\n", d.ImageID, d.Region, d.Count)
	}
	fmt.Printf("   render executions: %d of %d images (duplicate collapsed by deduplication ID)\n",
		renderRuns.Load(), len(batch))

	section("second gallery run (idempotent child IDs re-attach)")
	before := renderRuns.Load()
	runGallery(app, gallery, batch, "gallery-"+batchTag+"-rerun")
	fmt.Printf("   render executions during rerun: %d\n", renderRuns.Load()-before)
	fmt.Println("   img-1/2/4 re-attached to their finished renders via WithChildID;")
	fmt.Println("   img-3 was deduplicated onto img-1 in run 1, so it renders its own child now")

	section("strict gallery (WithCancelSiblings: a corrupt image cancels the surviving fleet)")
	strictBatch := []Image{
		{ID: "img-5", Region: "us", ContentHash: "slow-1"}, // a 3s render — wasted spend once the batch fails
		{ID: "img-6", Region: "eu", ContentHash: "slow-2"},
		{ID: "img-7", Region: "us", ContentHash: "poison"}, // fails immediately
	}
	start := time.Now()
	manifest = runGallery(app, strict, strictBatch, "strict-"+batchTag)
	fmt.Printf("   rescued with %d delivered in %s — the slow renders were cancelled,\n",
		len(manifest.Delivered), time.Since(start).Round(100*time.Millisecond))
	fmt.Println("   not drained: without the option the batch would settle only after every 3s render finished")
	for _, img := range strictBatch {
		status, err := duro.Status(app, strictChildID(batchTag, img.ID))
		if err != nil {
			fatal("child status: %v", err)
		}
		fmt.Printf("     %s → %s\n", img.ID, status.State)
	}
}

func runGallery(app *duro.App, gallery *duro.PipelineWorkflow[[]Image, Manifest], batch []Image, runID string) Manifest {
	handle, err := gallery.Start(app, batch, duro.WithWorkflowID(runID))
	if err != nil {
		fatal("starting gallery: %v", err)
	}
	manifest, err := handle.Result()
	if err != nil {
		fatal("gallery failed: %v", err)
	}
	return manifest
}

func databaseURL() string {
	if url := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); url != "" {
		return url
	}
	username := "postgres"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	return fmt.Sprintf("postgres://%s@localhost:5432/duro_thumbnails", username)
}

func section(title string) {
	fmt.Printf("\n── %s\n", title)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
