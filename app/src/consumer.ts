import express, { Request, Response } from "express";
import {
  Checkpoint,
  EventHubConsumerClient,
  PartitionProperties,
  ReceivedEventData,
  Subscription,
  earliestEventPosition,
} from "@azure/event-hubs";
import { BlobCheckpointStore } from "@azure/eventhubs-checkpointstore-blob";
import { ContainerClient } from "@azure/storage-blob";
import { EVENTHUBS_SCOPE, ManagedIdentityEndpointCredential, STORAGE_SCOPE, summariseToken } from "./identity";
import { Reading } from "./producer";
import { errorHandler, escapeHtml, handle, renderPage, renderTokenSummary, required } from "./web";

const RECENT_EVENTS = 25;

// What this process has seen on one partition since it started.
interface PartitionStats {
  // Where the checkpoint store said to resume from when this process started:
  // the sequence number of the last event processed before, or undefined if
  // there was no checkpoint (so reading began at the earliest event).
  resumedAfter?: number;
  received: number;
  firstSequenceNumber?: number;
  lastSequenceNumber?: number;
  lastOffset?: string;
  lastEnqueuedAt?: Date;
  lastCheckpoint?: { sequenceNumber: number; offset: string; at: Date };
  lastError?: string;
}

// Per-device ordering check: readings from one device share a partition key,
// so they should arrive with deviceSeq counting up by one.
interface DeviceStats {
  producerRun: string;
  partitions: Set<string>;
  received: number;
  lastDeviceSeq: number;
  outOfOrder: number;
  gaps: number;
}

interface RecentEvent {
  partitionId: string;
  sequenceNumber: number;
  offset: string;
  enqueuedAt: Date;
  reading: Reading;
}

export async function runConsumer(port: number): Promise<void> {
  const namespace = required("EVENTHUB_NAMESPACE");
  const eventHubName = required("EVENTHUB_NAME");
  const consumerGroup = required("EVENTHUB_CONSUMER_GROUP");
  const containerUrl = required("CHECKPOINT_CONTAINER_URL");

  // One credential serves both services: it asks the managed identity endpoint
  // for an Event Hubs token or a Storage token, depending on who's asking.
  const credential = new ManagedIdentityEndpointCredential(process.env.AZURE_CLIENT_ID);

  // Checkpoints and partition ownership are kept as blobs in this container,
  // under <namespace>/<event hub>/<consumer group>/.
  const checkpointStore = new BlobCheckpointStore(new ContainerClient(containerUrl, credential));

  const client = new EventHubConsumerClient(consumerGroup, namespace, eventHubName, credential, checkpointStore, {
    // With a checkpoint store, consumer instances share the partitions out
    // between them by claiming ownership. "greedy" claims every unowned
    // partition straight away, rather than one per load-balancing cycle
    // ("balanced", the default), so a single instance is reading all of them
    // within seconds of starting.
    loadBalancingOptions: { strategy: "greedy" },
  });

  const startedAt = new Date();
  const partitions = new Map<string, PartitionStats>();
  const devices = new Map<string, DeviceStats>();
  const recent: RecentEvent[] = [];
  let startupError: string | undefined;
  let subscription: Subscription | undefined;

  const statsFor = (partitionId: string): PartitionStats => {
    let stats = partitions.get(partitionId);
    if (!stats) {
      stats = { received: 0 };
      partitions.set(partitionId, stats);
    }
    return stats;
  };

  const track = (partitionId: string, event: ReceivedEventData): void => {
    const stats = statsFor(partitionId);
    stats.received++;
    stats.firstSequenceNumber ??= event.sequenceNumber;
    stats.lastSequenceNumber = event.sequenceNumber;
    stats.lastOffset = String(event.offset);
    stats.lastEnqueuedAt = event.enqueuedTimeUtc;

    const reading = event.body as Reading;
    let device = devices.get(reading.deviceId);
    if (!device || device.producerRun !== reading.producerRun) {
      device = { producerRun: reading.producerRun, partitions: new Set(), received: 0, lastDeviceSeq: 0, outOfOrder: 0, gaps: 0 };
      devices.set(reading.deviceId, device);
    } else if (reading.deviceSeq <= device.lastDeviceSeq) {
      device.outOfOrder++;
    } else if (reading.deviceSeq > device.lastDeviceSeq + 1) {
      device.gaps++;
    }
    device.partitions.add(partitionId);
    device.received++;
    device.lastDeviceSeq = Math.max(device.lastDeviceSeq, reading.deviceSeq);

    recent.unshift({
      partitionId,
      sequenceNumber: event.sequenceNumber,
      offset: String(event.offset),
      enqueuedAt: event.enqueuedTimeUtc,
      reading,
    });
    recent.length = Math.min(recent.length, RECENT_EVENTS);
  };

  const start = async (): Promise<void> => {
    // Read the checkpoints this consumer group left behind before subscribing,
    // purely to show on the page where each partition resumes from. The
    // subscription reads them again itself when it claims each partition.
    // Retried until it succeeds: subscribing without knowing them would leave
    // the page unable to show whether the restart resumed.
    let existing: Checkpoint[];
    for (;;) {
      try {
        existing = await checkpointStore.listCheckpoints(client.fullyQualifiedNamespace, client.eventHubName, consumerGroup, {
          abortSignal: AbortSignal.timeout(15_000),
        });
        startupError = undefined;
        break;
      } catch (err) {
        startupError = `Couldn't list checkpoints (retrying in 10s): ${(err as Error).message}`;
        console.error(startupError);
        await new Promise((resolve) => setTimeout(resolve, 10_000));
      }
    }
    for (const checkpoint of existing) {
      statsFor(checkpoint.partitionId).resumedAfter = checkpoint.sequenceNumber;
    }
    console.log(
      `found ${existing.length} checkpoints at start-up: ${existing.map((c) => `partition ${c.partitionId} @ ${c.sequenceNumber}`).join(", ") || "none"}`,
    );

    subscription = client.subscribe(
      {
        processEvents: async (events, context) => {
          // Called with an empty array when maxWaitTimeInSeconds passes with
          // nothing new - there's nothing to process or checkpoint.
          if (events.length === 0) {
            return;
          }
          for (const event of events) {
            track(context.partitionId, event);
          }
          // Checkpoint only after the batch has been processed: if the process
          // dies before this, the batch is read again on restart (at-least-once).
          const last = events[events.length - 1];
          await context.updateCheckpoint(last);
          statsFor(context.partitionId).lastCheckpoint = {
            sequenceNumber: last.sequenceNumber,
            offset: String(last.offset),
            at: new Date(),
          };
          console.log(`partition ${context.partitionId}: processed ${events.length} events, checkpointed at ${last.sequenceNumber}`);
        },
        processError: async (err, context) => {
          console.error(`error on partition ${context.partitionId || "(load balancing)"}:`, err);
          if (context.partitionId) {
            statsFor(context.partitionId).lastError = err.message;
          } else {
            startupError = err.message;
          }
        },
        processInitialize: async (context) => {
          console.log(`claimed partition ${context.partitionId}`);
          statsFor(context.partitionId);
        },
        processClose: async (reason, context) => {
          console.log(`released partition ${context.partitionId}: ${reason}`);
        },
      },
      {
        // Only used for a partition with no checkpoint. Starting from the
        // earliest event (rather than the default, the latest) means nothing
        // sent before the consumer first started is missed - and it makes a
        // resumed restart easy to tell apart from re-reading everything.
        startPosition: earliestEventPosition,
        maxBatchSize: 100,
        maxWaitTimeInSeconds: 5,
      },
    );
  };

  start().catch((err: Error) => {
    console.error("failed to start reading:", err);
    startupError = err.message;
  });

  // On shutdown (App Service sends SIGTERM when stopping or restarting a
  // container), close the subscription so this instance gives up ownership of
  // its partitions straight away. Otherwise its replacement waits for the
  // ownership records to expire before it can claim them.
  process.once("SIGTERM", () => {
    console.log("SIGTERM received, closing the subscription");
    Promise.resolve(subscription?.close())
      .then(() => client.close())
      .catch((err) => console.error("error while closing:", err))
      .finally(() => process.exit(0));
  });

  const app = express();

  app.get("/healthz", (_req: Request, res: Response) => {
    res.status(200).send("ok");
  });

  app.get("/", handle(async (_req, res) => {
    // The newest sequence number in each partition, to show how far behind the
    // consumer is. Bounded, so the page reports an unreachable Event Hub rather
    // than hanging while the client retries.
    let hubError: Error | undefined;
    const latest = new Map<string, PartitionProperties>();
    try {
      const abortSignal = AbortSignal.timeout(8_000);
      const ids = await client.getPartitionIds({ abortSignal });
      for (const p of await Promise.all(ids.map((id) => client.getPartitionProperties(id, { abortSignal })))) {
        latest.set(p.partitionId, p);
        statsFor(p.partitionId);
      }
    } catch (err) {
      hubError = err as Error;
    }

    const [eventhubsToken, storageToken] = await Promise.all(
      [EVENTHUBS_SCOPE, STORAGE_SCOPE].map((scope) =>
        credential
          .getToken(scope)
          .then((t) => summariseToken(t.token))
          .catch((err: Error) => err),
      ),
    );

    const fmt = (n: number | undefined) => (n === undefined ? "-" : String(n));
    const time = (d: Date | undefined) => (d ? escapeHtml(d.toISOString().slice(11, 23)) : "-");
    const partitionRows = [...partitions.entries()]
      .sort(([a], [b]) => Number(a) - Number(b))
      .map(([id, s]) => {
        const p = latest.get(id);
        const lag = p && !p.isEmpty ? p.lastEnqueuedSequenceNumber - (s.lastCheckpoint?.sequenceNumber ?? s.resumedAfter ?? -1) : 0;
        return `
        <tr>
          <td class="num">${escapeHtml(id)}</td>
          <td>${s.resumedAfter === undefined ? "earliest (no checkpoint)" : `after seq ${s.resumedAfter}`}</td>
          <td class="num">${s.received}</td>
          <td class="num">${fmt(s.firstSequenceNumber)}</td>
          <td class="num">${fmt(s.lastSequenceNumber)}<br><small>offset ${escapeHtml(s.lastOffset ?? "-")}</small></td>
          <td class="num">${s.lastCheckpoint ? `${s.lastCheckpoint.sequenceNumber}<br><small>${time(s.lastCheckpoint.at)}</small>` : "-"}</td>
          <td class="num">${p ? (p.isEmpty ? "empty" : p.lastEnqueuedSequenceNumber) : "-"}</td>
          <td class="num">${p ? lag : "-"}</td>
        </tr>
        ${s.lastError ? `<tr><td></td><td colspan="7" class="error">${escapeHtml(s.lastError)}</td></tr>` : ""}`;
      })
      .join("");

    const deviceRows = [...devices.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(
        ([id, d]) => `
        <tr>
          <td><code>${escapeHtml(id)}</code></td>
          <td>${escapeHtml([...d.partitions].join(", "))}</td>
          <td class="num">${d.received}</td>
          <td class="num">${d.lastDeviceSeq}</td>
          <td class="num ${d.outOfOrder ? "bad" : ""}">${d.outOfOrder}</td>
          <td class="num">${d.gaps}</td>
        </tr>`,
      )
      .join("");

    const recentRows = recent
      .map(
        (e) => `
        <tr>
          <td class="num">${escapeHtml(e.partitionId)}</td>
          <td class="num">${e.sequenceNumber}<br><small>offset ${escapeHtml(e.offset)}</small></td>
          <td><code>${escapeHtml(String(e.reading.deviceId))}</code> #${escapeHtml(String(e.reading.deviceSeq))}</td>
          <td class="num">${escapeHtml(String(e.reading.temperature))} &deg;C</td>
          <td><small>${time(e.enqueuedAt)}</small></td>
        </tr>`,
      )
      .join("");

    const body = `
      <h1>Consumer</h1>
      <p class="flow">producer &rarr; Event Hub <code>${escapeHtml(eventHubName)}</code> &rarr; <strong>consumer group <code>${escapeHtml(consumerGroup)}</code></strong> (managed identity endpoint &rarr; Entra token) &rarr; checkpoints in Blob Storage</p>
      <p>Started at ${escapeHtml(startedAt.toISOString())}. Counts are since this process started. This page refreshes every 3 seconds.</p>
      ${startupError ? `<p class="error">${escapeHtml(startupError)}</p>` : ""}
      ${hubError ? `<p class="error">Couldn't read the Event Hub's partition properties: ${escapeHtml(hubError.message)}</p>` : ""}
      <h2>Partitions</h2>
      <table>
        <thead><tr><th class="num">Partition</th><th>Resumed from</th><th class="num">Received</th><th class="num">First seq</th><th class="num">Last seq</th><th class="num">Checkpoint</th><th class="num">Latest in hub</th><th class="num">Behind</th></tr></thead>
        <tbody>${partitionRows || `<tr><td colspan="8">No partitions claimed yet.</td></tr>`}</tbody>
      </table>
      <h2>Devices</h2>
      <p>Each device's readings share a partition key, so they should all land on one partition and arrive in order.</p>
      <table>
        <thead><tr><th>Device</th><th>Partition</th><th class="num">Received</th><th class="num">Last seq</th><th class="num">Out of order</th><th class="num">Gaps</th></tr></thead>
        <tbody>${deviceRows || `<tr><td colspan="6">No events yet - send a burst from the producer.</td></tr>`}</tbody>
      </table>
      <h2>Recent events</h2>
      <table>
        <thead><tr><th class="num">Partition</th><th class="num">Seq</th><th>Reading</th><th class="num">Value</th><th>Enqueued</th></tr></thead>
        <tbody>${recentRows || `<tr><td colspan="5">None yet.</td></tr>`}</tbody>
      </table>
      ${renderTokenSummary("Entra token for Event Hubs (from the managed identity endpoint)", eventhubsToken)}
      ${renderTokenSummary("Entra token for Blob Storage (from the managed identity endpoint)", storageToken)}`;
    res.send(renderPage("Consumer", body, 3));
  }));

  app.use(errorHandler);

  app.listen(port, "0.0.0.0", () => {
    console.log(`consumer listening on 0.0.0.0:${port}, reading event hub ${eventHubName} on ${namespace} as consumer group ${consumerGroup}`);
  });
}
