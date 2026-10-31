output "producer_url" {
  description = "Where to reach the producer (send telemetry) in a browser."
  value       = "https://${azurerm_linux_web_app.producer.default_hostname}"
}

output "consumer_url" {
  description = "Where to reach the consumer (per-partition stats) in a browser."
  value       = "https://${azurerm_linux_web_app.consumer.default_hostname}"
}

output "resource_group" {
  description = "The resource group containing the deployed resources."
  value       = azurerm_resource_group.main.name
}

output "eventhub_namespace" {
  description = "The Event Hubs Namespace the producer sends to."
  value       = azurerm_eventhub_namespace.main.name
}

output "storage_account" {
  description = "The Storage Account whose container holds the consumer's checkpoints."
  value       = azurerm_storage_account.main.name
}
