# ---------------------------------------------------------------------------
# App Service Plan - houses both App Services.
# ---------------------------------------------------------------------------
resource "azurerm_service_plan" "main" {
  name                = "${var.name_prefix}-plan"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = var.app_service_sku
}

# ---------------------------------------------------------------------------
# Producer - a web page that sends bursts of simulated telemetry to the Event
# Hub.
#
# It runs as the producer identity, which may only send to the Event Hub. App
# Service exposes that identity to the container through the managed identity
# endpoint (the IDENTITY_ENDPOINT and IDENTITY_HEADER environment variables),
# which the app calls to obtain an Entra access token for Event Hubs.
# ---------------------------------------------------------------------------
resource "azurerm_linux_web_app" "producer" {
  name                = "${var.name_prefix}-producer"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_service_plan.main.location
  service_plan_id     = azurerm_service_plan.main.id
  https_only          = true
  tags                = var.tags

  ftp_publish_basic_authentication_enabled       = false
  webdeploy_publish_basic_authentication_enabled = false

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.producer.id]
  }

  site_config {
    health_check_path                 = "/healthz"
    health_check_eviction_time_in_min = 2

    application_stack {
      docker_image_name        = "${local.image_name}:${local.image_tag}"
      docker_registry_url      = "https://${azurerm_container_registry.main.login_server}"
      docker_registry_username = azurerm_container_registry.main.admin_username
      docker_registry_password = azurerm_container_registry.main.admin_password
    }
  }

  app_settings = {
    ROLE = "producer"

    # App Service routes public traffic to the container on this port, and the
    # app binds to it (12-factor port binding).
    WEBSITES_PORT = tostring(local.app_port)
    PORT          = tostring(local.app_port)

    # Where to send. There's no connection string: the namespace only accepts
    # Entra access tokens.
    EVENTHUB_NAMESPACE = local.eventhub_hostname
    EVENTHUB_NAME      = azurerm_eventhub.telemetry.name

    # Selects which of the web app's identities the managed identity endpoint
    # issues a token for.
    AZURE_CLIENT_ID = azurerm_user_assigned_identity.producer.client_id

    # Linked from the producer's page, so you can follow a burst through.
    CONSUMER_URL = "https://${azurerm_linux_web_app.consumer.default_hostname}"
  }

  # The image must exist in the registry, and the identity must hold its role,
  # before the app starts.
  depends_on = [
    terraform_data.build_and_push,
    azurerm_role_assignment.producer_sender,
  ]
}

# ---------------------------------------------------------------------------
# Consumer - reads every partition of the Event Hub through the "dashboard"
# consumer group, checkpointing to Blob Storage as it goes.
#
# It runs as the consumer identity, which may read from the Event Hub and
# read and write blobs in the checkpoint container, but not send. It also
# serves a page with per-partition stats. Always On keeps the container
# running, so it's always reading rather than only when the page is visited.
# ---------------------------------------------------------------------------
resource "azurerm_linux_web_app" "consumer" {
  name                = "${var.name_prefix}-consumer"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_service_plan.main.location
  service_plan_id     = azurerm_service_plan.main.id
  https_only          = true
  tags                = var.tags

  ftp_publish_basic_authentication_enabled       = false
  webdeploy_publish_basic_authentication_enabled = false

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.consumer.id]
  }

  site_config {
    always_on                         = true
    health_check_path                 = "/healthz"
    health_check_eviction_time_in_min = 2

    application_stack {
      docker_image_name        = "${local.image_name}:${local.image_tag}"
      docker_registry_url      = "https://${azurerm_container_registry.main.login_server}"
      docker_registry_username = azurerm_container_registry.main.admin_username
      docker_registry_password = azurerm_container_registry.main.admin_password
    }
  }

  app_settings = {
    ROLE = "consumer"

    WEBSITES_PORT = tostring(local.app_port)
    PORT          = tostring(local.app_port)

    EVENTHUB_NAMESPACE      = local.eventhub_hostname
    EVENTHUB_NAME           = azurerm_eventhub.telemetry.name
    EVENTHUB_CONSUMER_GROUP = azurerm_eventhub_consumer_group.dashboard.name

    # Where to keep checkpoints. There's no account key: shared key access is
    # disabled.
    CHECKPOINT_CONTAINER_URL = "${azurerm_storage_account.main.primary_blob_endpoint}${azurerm_storage_container.checkpoints.name}"

    AZURE_CLIENT_ID = azurerm_user_assigned_identity.consumer.client_id
  }

  depends_on = [
    terraform_data.build_and_push,
    azurerm_role_assignment.consumer_receiver,
    azurerm_role_assignment.consumer_checkpoints,
  ]
}
