# ---------------------------------------------------------------------------
# Event Hubs Namespace - holds the Event Hub the telemetry flows through.
#
# Local (SAS key) authentication is disabled, so there's no connection string
# to leak: the only way in is an Entra access token, which each app obtains
# from the App Service managed identity endpoint.
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_namespace" "main" {
  name                         = "${var.name_prefix}-ns"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  sku                          = "Standard"
  capacity                     = 1
  local_authentication_enabled = false
  minimum_tls_version          = "1.2"
  tags                         = var.tags
}

# ---------------------------------------------------------------------------
# The Event Hub - an append-only log split into partitions.
#
# The producer sends each event with its device ID as the partition key. Every
# event with the same key lands in the same partition, and a partition is read
# back in the order it was written - so each device's readings stay in order,
# while different devices spread across the partitions and are read in
# parallel. There's no ordering guarantee across partitions.
# ---------------------------------------------------------------------------
resource "azurerm_eventhub" "telemetry" {
  name            = "telemetry"
  namespace_id    = azurerm_eventhub_namespace.main.id
  partition_count = var.partition_count

  # Events stay readable for a day whether or not anyone has read them: unlike
  # a queue, reading an event doesn't remove it.
  message_retention = 1
}

# ---------------------------------------------------------------------------
# A consumer group is an independent view of the Event Hub, with its own
# position in each partition. The consumer reads through a dedicated group
# rather than $Default, so another reader (say, an analytics job) can be added
# on its own group without the two moving each other's checkpoints.
# ---------------------------------------------------------------------------
resource "azurerm_eventhub_consumer_group" "dashboard" {
  name                = "dashboard"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.main.name
}

# ---------------------------------------------------------------------------
# Data-plane role assignments, scoped to the Event Hub rather than the
# namespace.
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "producer_sender" {
  scope                = azurerm_eventhub.telemetry.id
  role_definition_name = "Azure Event Hubs Data Sender"
  principal_id         = azurerm_user_assigned_identity.producer.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "consumer_receiver" {
  scope                = azurerm_eventhub.telemetry.id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = azurerm_user_assigned_identity.consumer.principal_id
  principal_type       = "ServicePrincipal"
}
