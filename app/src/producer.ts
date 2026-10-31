import { randomUUID } from "node:crypto";
import express, { Request, Response } from "express";
import { EventData, EventHubProducerClient } from "@azure/event-hubs";
import { EVENTHUBS_SCOPE, ManagedIdentityEndpointCredential, summariseToken } from "./identity";
import { errorHandler, escapeHtml, handle, renderPage, renderTokenSummary, required } from "./web";

// One simulated telemetry reading, as the producer sends it and the consumer
// reads it back.
export interface Reading {
  deviceId: string;
  // Counts up from 1 for each device, so the consumer can check that a
  // device's readings arrive in the order they were sent.
  deviceSeq: number;
  // Changes whenever the producer restarts (which resets deviceSeq).
  producerRun: string;
  temperature: number;
  sentAt: string;
}

const MAX_EVENTS = 1000;
const MAX_DEVICES = 20;

interface BurstSummary {
  events: number;
  devices: number;
  batches: number;
  milliseconds: number;
  // Each device's first and last deviceSeq in this burst.
  ranges: { deviceId: string; first: number; last: number }[];
}

function deviceName(index: number): string {
  return `device-${String(index + 1).padStart(2, "0")}`;
}

function clamp(value: unknown, min: number, max: number, fallback: number): number {
  const n = Math.floor(Number(value));
  return Number.isFinite(n) ? Math.min(Math.max(n, min), max) : fallback;
}

export async function runProducer(port: number): Promise<void> {
  const namespace = required("EVENTHUB_NAMESPACE");
  const eventHubName = required("EVENTHUB_NAME");
  const consumerUrl = process.env.CONSUMER_URL;

  // The credential calls the managed identity endpoint for a token whenever
  // the Event Hubs client needs one - there's no connection string or SAS key.
  const credential = new ManagedIdentityEndpointCredential(process.env.AZURE_CLIENT_ID);
  const producer = new EventHubProducerClient(namespace, eventHubName, credential);

  const producerRun = randomUUID().slice(0, 8);
  const nextSeq = new Map<string, number>();
  let lastBurst: BurstSummary | undefined;

  const app = express();
  app.use(express.urlencoded({ extended: false }));

  app.get("/healthz", (_req: Request, res: Response) => {
    res.status(200).send("ok");
  });

  app.get("/", handle(async (req, res) => {
    // Fetch (or reuse) the same token the Event Hubs client sends, to show its claims.
    const summary = await credential
      .getToken(EVENTHUBS_SCOPE)
      .then((t) => summariseToken(t.token))
      .catch((err: Error) => err);

    const error = typeof req.query.error === "string" ? req.query.error : undefined;
    const burst = req.query.sent !== undefined ? lastBurst : undefined;
    const ranges = (burst?.ranges ?? [])
      .map(
        (r) => `<tr><td><code>${escapeHtml(r.deviceId)}</code></td><td class="num">${r.first}</td><td class="num">${r.last}</td></tr>`,
      )
      .join("");

    const body = `
      <h1>Producer</h1>
      <p class="flow">App Service &rarr; managed identity endpoint &rarr; Entra token &rarr; <strong>Event Hub <code>${escapeHtml(eventHubName)}</code></strong> &rarr; consumer (checkpoints in Blob Storage)</p>
      <p>Sends a burst of simulated temperature readings. Each event's partition key is its device ID, so every reading from one device goes to the same partition, in order.</p>
      ${error ? `<p class="error">Couldn't send: ${escapeHtml(error)}</p>` : ""}
      ${burst ? `<p class="notice">Sent ${burst.events} events from ${burst.devices} devices in ${burst.batches} batches (${burst.milliseconds} ms).${consumerUrl ? ` <a href="${escapeHtml(consumerUrl)}">See them arrive at the consumer &rarr;</a>` : ""}</p>` : ""}
      <form class="send" method="post" action="/send">
        <label>Events <input name="events" type="number" min="1" max="${MAX_EVENTS}" value="100"></label>
        <label>Devices <input name="devices" type="number" min="1" max="${MAX_DEVICES}" value="8"></label>
        <button type="submit">Send burst</button>
      </form>
      ${ranges ? `<table><thead><tr><th>Device</th><th class="num">First seq</th><th class="num">Last seq</th></tr></thead><tbody>${ranges}</tbody></table>` : ""}
      <p><small>Producer run <code>${escapeHtml(producerRun)}</code> - device sequence numbers start again from 1 when the producer restarts.</small></p>
      ${consumerUrl ? `<p><a href="${escapeHtml(consumerUrl)}">Open the consumer</a> to see per-partition stats.</p>` : ""}
      ${renderTokenSummary("Entra token for Event Hubs (from the managed identity endpoint)", summary)}`;
    res.send(renderPage("Producer", body));
  }));

  app.post("/send", handle(async (req, res) => {
    const events = clamp(req.body.events, 1, MAX_EVENTS, 100);
    const devices = clamp(req.body.devices, 1, MAX_DEVICES, 8);
    const started = Date.now();

    // Readings are generated round-robin across the devices, then grouped by
    // device: a batch can only carry one partition key.
    const byDevice = new Map<string, EventData[]>();
    const ranges: BurstSummary["ranges"] = [];
    for (let i = 0; i < events; i++) {
      const deviceId = deviceName(i % devices);
      const deviceSeq = nextSeq.get(deviceId) ?? 1;
      nextSeq.set(deviceId, deviceSeq + 1);
      const reading: Reading = {
        deviceId,
        deviceSeq,
        producerRun,
        temperature: Math.round((18 + Math.random() * 10) * 10) / 10,
        sentAt: new Date().toISOString(),
      };
      const list = byDevice.get(deviceId) ?? [];
      list.push({ body: reading, contentType: "application/json" });
      byDevice.set(deviceId, list);
    }

    try {
      let batches = 0;
      for (const [deviceId, deviceEvents] of byDevice) {
        // One device's batches go out one after another, so they're appended
        // to the partition in sequence order.
        const abortSignal = AbortSignal.timeout(15_000);
        let batch = await producer.createBatch({ partitionKey: deviceId, abortSignal });
        for (const event of deviceEvents) {
          if (!batch.tryAdd(event)) {
            await producer.sendBatch(batch, { abortSignal });
            batches++;
            batch = await producer.createBatch({ partitionKey: deviceId, abortSignal });
            batch.tryAdd(event);
          }
        }
        await producer.sendBatch(batch, { abortSignal });
        batches++;
        const seqs = deviceEvents.map((e) => (e.body as Reading).deviceSeq);
        ranges.push({ deviceId, first: seqs[0], last: seqs[seqs.length - 1] });
      }
      lastBurst = { events, devices, batches, milliseconds: Date.now() - started, ranges };
      console.log(`sent ${events} events from ${devices} devices in ${batches} batches to event hub ${eventHubName}`);
      res.redirect(303, "/?sent=1");
    } catch (err) {
      console.error(`failed to send to event hub ${eventHubName}:`, err);
      res.redirect(303, `/?error=${encodeURIComponent((err as Error).message)}`);
    }
  }));

  app.use(errorHandler);

  app.listen(port, "0.0.0.0", () => {
    console.log(`producer listening on 0.0.0.0:${port}, sending to event hub ${eventHubName} on ${namespace}`);
  });
}
