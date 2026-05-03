# Client API

## Multipart job submission

Jobs are submitted via `POST /api/v1/jobs` with multipart form data. The `wasm` field is required. Either `input` (file upload) or `generate_mb` (random data size) must be provided, but not both. The `num_reduces` field defaults to 4.

Multipart is used instead of JSON because WASM binaries and input files are opaque byte sequences. Encoding them as base64 in JSON would inflate the payload by 33%. Multipart streams the data directly to the master's disk.

## Input validation at boundaries

The master validates the multipart fields: WASM must be present, and either `input` or `generate_mb` must be set. Missing both returns 400. The client also validates this locally and exits with an error message before making the request. Dual validation gives a clear error to the user at the CLI level and a safe guard on the server side.

## Status polling

`GET /api/v1/jobs/{id}/status` returns a JSON object with `status` (pending/running/failed/done), `map_progress` (0.0-1.0), and `reduce_progress` (0.0-1.0). Progress is computed as the fraction of completed tasks. The read lock on state is held only during the count, so polling never blocks the scheduler.

## One-time result download

`GET /api/v1/jobs/{id}/result` returns the merged output. If the job is not yet done, 418 (I'm a Teapot) is returned. If already downloaded, 404 is returned. The one-time design simplifies cleanup - the master does not need to track whether the client finished downloading or handle partial downloads.

## CLI structure

The client binary uses clap subcommands: `submit`, `status`, `result`. The `--master` flag sets the base URL and defaults to `http://127.0.0.1:3000`. Each subcommand is a single HTTP call. No retries, no automatic polling loops - the caller controls timing. This keeps the client thin and scriptable.
