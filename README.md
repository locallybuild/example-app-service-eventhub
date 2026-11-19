```
ooooo                                      oooo  oooo              
`888'                                      `888  `888              
 888          .ooooo.   .ooooo.   .oooo.    888   888  oooo    ooo 
 888         d88' `88b d88' `"Y8 `P  )88b   888   888   `88.  .8'
 888         888   888 888        .oP"888   888   888    `88..8'
 888       o 888   888 888   .o8 d8(  888   888   888     `888'
o888ooooood8 `Y8bod8P' `Y8bod8P' `Y888""8o o888o o888o     .8'
                                                       .o..P'
                                                       `Y8P'
```

# Example: Stream telemetry from App Service through Event Hubs, with checkpoints in Blob Storage, within Locally

This example shows how to stream telemetry between two apps on App Service through Azure Event Hubs, with checkpoints in Azure Blob Storage, on [Locally Build](https://locally.build). The consumer records how far it has read in Azure Blob Storage, so it carries on where it left off after a restart. Neither app stores a key, connection string or password: each authenticates with its own **user-assigned managed identity**.

```
 producer (App Service)                         consumer (App Service)
   │                                               │
   ├─ GET $IDENTITY_ENDPOINT ─▶ Entra token        ├─ GET $IDENTITY_ENDPOINT ─▶ Entra token (Event Hubs)
   │    (aud: eventhubs.azure.net)                 ├─ GET $IDENTITY_ENDPOINT ─▶ Entra token (Storage)
   │                                               │
   └─ send ─▶ Event Hub "telemetry" ─▶ read (consumer group "dashboard")
              (4 partitions, partition key        └─ checkpoint ─▶ Blob container "checkpoints"
               = device ID)
```

There are two apps, written in TypeScript (Node.js and Express). Both run from one image, and the `ROLE` app setting decides which one a container runs as. The source lives in [`app/`](./app), alongside the infrastructure that runs it.

* **producer** - a page where you pick a number of events and devices. Clicking Send burst sends that many simulated temperature readings, spread round-robin across the devices.
* **consumer** - reads every partition of the Event Hub and serves a page, refreshed every few seconds, with per-partition stats (events received, last sequence number and offset, last checkpoint, how far behind it is), a per-device ordering check, and the most recent events.

### Partitions and ordering

The Event Hub has 4 partitions. The producer sends each reading with its device ID as the **partition key**, so every reading from one device lands on the same partition. A partition is read back in the order it was written, so each device's readings arrive in order, while different devices are spread across the partitions and read in parallel. There's no ordering across partitions.

Each reading carries a per-device sequence number. The consumer's Devices table shows which partition each device landed on and counts any reading that arrives out of order (there should be none).

### Consumer groups and checkpoints

Event Hubs keeps events for the retention period whether or not anyone has read them: reading an event doesn't remove it, as it would from a queue. Each reader keeps track of its own position in each partition.

* The consumer reads through its own **consumer group**, `dashboard`, rather than `$Default`. Another reader could be added on a different group without either one moving the other's position.
* After processing each batch, the consumer writes a **checkpoint** - the sequence number and offset of the last event it processed - for that partition, as a blob in the `checkpoints` container. This is done by `BlobCheckpointStore` from [`@azure/eventhubs-checkpointstore-blob`](https://www.npmjs.com/package/@azure/eventhubs-checkpointstore-blob).
* When it starts, the consumer resumes each partition from just after its checkpoint. A partition with no checkpoint is read from the earliest event still retained.

The same container holds partition **ownership** records. If you ran several consumer instances, they would use these to share the partitions out between them.

The consumer page's "Resumed from" column shows the checkpoint each partition started from. After a restart, "First seq" is one more than that and "Received" counts only the events sent since the last checkpoint, rather than everything in the Event Hub.

### How the apps get their tokens

When a web app has a managed identity, App Service sets two environment variables in its container:

* `IDENTITY_ENDPOINT` - a local URL that issues Entra access tokens for that identity.
* `IDENTITY_HEADER` - a secret that must be sent with each request.

[`app/src/identity.ts`](./app/src/identity.ts) calls that endpoint directly: one HTTP `GET` asking for a token for a resource (`https://eventhubs.azure.net` or `https://storage.azure.com`). It does this instead of using `DefaultAzureCredential` from `@azure/identity`, which makes the same call but hides it. The Event Hubs and Blob Storage clients accept the result as an ordinary `TokenCredential`. Each app's page shows the claims of the tokens it was issued (audience, issuer, object ID, expiry), but never the token itself.

### Least privilege

Each app has its own identity, and each identity has only the roles that app needs:

| Identity | Role | Scope |
| --- | --- | --- |
| producer | Azure Event Hubs Data Sender | the `telemetry` Event Hub |
| consumer | Azure Event Hubs Data Receiver | the `telemetry` Event Hub |
| consumer | Storage Blob Data Contributor | the Storage Account |

Shared-key and SAS authentication are switched off on both the Event Hubs Namespace (`local_authentication_enabled = false`) and the Storage Account (`shared_access_key_enabled = false`), so an Entra token is the only way in.

On Azure the Storage Blob Data Contributor role could be scoped to just the `checkpoints` container. Locally currently only honours Storage data-plane roles assigned on the account or above (a container-scoped assignment gets `AuthorizationPermissionMismatch`), so the example assigns it on the account, which holds nothing else.

## Requirements

* [Locally Build](https://locally.build).
* Either [HashiCorp Terraform](https://terraform.io) or [OpenTofu](https://opentofu.org).
* Either [Docker](https://www.docker.com) or [Podman](https://podman.io) (recommended).
* The Locally Plugin for `Microsoft.EventHub` installed (`locally plugin install --name Microsoft.EventHub`).
* The Locally Plugin for `Microsoft.Storage` installed (`locally plugin install --name Microsoft.Storage`).
* The Locally Plugin for `Microsoft.Web` installed (`locally plugin install --name Microsoft.Web`).
* The Locally Plugin for `Microsoft.ContainerRegistry` installed (`locally plugin install --name Microsoft.ContainerRegistry`).
* The Locally Plugin for `Microsoft.Insights` installed (`locally plugin install --name Microsoft.Insights`).
* The Locally Plugin for `Microsoft.OperationalInsights` installed (`locally plugin install --name Microsoft.OperationalInsights`).

## Running the example

First up, we need to ensure our container runtime (Docker or Podman) is running, then launch Locally:

```bash
locally build
```

With Locally running, in another terminal we can initialise Terraform, which both downloads the providers we need and configures the module for use:

```bash
cd environments/locally
terraform init
```

> [!NOTE]
> You can use OpenTofu instead by running `tofu` wherever this says `terraform`.

With Terraform initialised, we can then provision the example by running:

```bash
locally run terraform apply
```

As part of the apply, the app is built with `docker build` and pushed into the Container Registry, so the `docker` command needs to be available (with Podman, the `podman-docker` shim or a `docker` alias works).

Once you approve the plan and the resources have been deployed, both apps are running at the URLs in the outputs:

```
producer_url = "https://locally-example-eventhub-producer.furnace.locally:5663"
consumer_url = "https://locally-example-eventhub-consumer.furnace.locally:5663"
```

[Open the producer](https://locally-example-eventhub-producer.furnace.locally:5663) and send a burst, then follow the link to [the consumer](https://locally-example-eventhub-consumer.furnace.locally:5663). Within a few seconds the events show up there, spread across the 4 partitions, with each device's readings on a single partition.

To see the checkpoints at work, restart the consumer:

```bash
locally run az webapp restart --resource-group locally-example-eventhub-resources --name locally-example-eventhub-consumer
```

When it comes back, its page shows each partition resuming from its checkpoint, and nothing already processed is read again. Send another burst and only the new events are received.

---

You can also look at each step from the CLI. To peek at the events in a partition (they stay there after the consumer has read them):

```bash
locally eventhub hub peek --namespace locally-example-eventhub-ns --name telemetry --partition 0
```

As this Terraform configuration sends the App Service logs into a Log Analytics Workspace, we can then query them within [the Locally Dashboard, in the Monitoring section](https://localhost:5678/monitoring/components), by running:

```
AppServiceConsoleLogs | order by TimeGenerated desc
```

## Notes

This example has no sign-in of its own: anyone who can reach the producer can send events, and anyone who can reach the consumer can see them. That keeps the example focused on the identity → Event Hubs → Blob Storage wiring. A real app would sit behind authentication.

The consumer checkpoints after it has processed a batch, not before. If it stops between the two, that batch is read again when it restarts, so processing is at-least-once: a real consumer should be able to handle the same event twice.

The consumer uses the `greedy` load-balancing strategy, which claims every unowned partition at once, rather than the default `balanced` strategy, which claims one per cycle. On shutdown it closes its subscription, which gives up ownership of its partitions so a replacement can claim them straight away.

`azurerm_eventhub_namespace` doesn't export the namespace's hostname, so the module reads it from the `Endpoint=sb://<host>/` part of the namespace's connection string. Only the hostname is used.

Because shared-key access is disabled on the Storage Account, the provider sets `storage_use_azuread = true` so that Terraform also uses Entra ID to manage the container.

### Known issue on Locally

Locally currently returns blob ETags in List Blobs results in a different form from Azure, so `BlobCheckpointStore` can't update a partition ownership blob once it exists: the conditional write fails with `412 ConditionNotMet`, and the SDK quietly treats the partition as owned by someone else rather than raising an error. The first time the consumer starts, it creates the ownership blobs and processes every partition as normal. After a restart, it finds its checkpoints but processes no partitions - nothing appears in its logs or on its page - until the old ownership blobs are deleted from the `checkpoints` container. On Azure Storage the consumer takes over its partitions straight away.

## Tearing it down

```bash
cd environments/locally
locally run terraform destroy
```
