# ---------------------------------------------------------------------------
# Storage Account - holds the consumer's checkpoints.
#
# Event Hubs doesn't remember how far a reader has got: that's the reader's
# job. The consumer records, per partition, the last event it finished
# processing (a "checkpoint") as a blob in this account. When it restarts, it
# carries on from those checkpoints rather than from the start of the stream.
# The same container also holds partition ownership records, which is how
# several consumer instances would split the partitions between them.
#
# Shared key access is disabled, so the consumer authenticates with an Entra
# access token for its managed identity rather than an account key.
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "main" {
  name                            = substr(replace(var.name_prefix, "-", ""), 0, 24)
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_id    = azurerm_storage_account.main.id
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Data-plane role assignment - lets the consumer read and write the checkpoint
# and ownership blobs.
#
# On Azure this could be scoped to just the checkpoints container. Locally
# only honours Storage data-plane roles assigned on the Storage Account (or
# above), so it's scoped to the account here - the account holds nothing else.
# ---------------------------------------------------------------------------
resource "azurerm_role_assignment" "consumer_checkpoints" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.consumer.principal_id
  principal_type       = "ServicePrincipal"
}
