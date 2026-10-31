output "producer_url" {
  description = "Where to reach the producer (send telemetry) in a browser."
  value       = module.app-service-eventhub.producer_url
}

output "consumer_url" {
  description = "Where to reach the consumer (per-partition stats) in a browser."
  value       = module.app-service-eventhub.consumer_url
}

output "eventhub_namespace" {
  description = "The Event Hubs Namespace the producer sends to."
  value       = module.app-service-eventhub.eventhub_namespace
}

output "storage_account" {
  description = "The Storage Account whose container holds the consumer's checkpoints."
  value       = module.app-service-eventhub.storage_account
}
