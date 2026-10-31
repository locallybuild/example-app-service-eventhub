terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.0"
    }
  }
}

provider "azurerm" {
  features {}

  # The Storage Account has shared key access disabled, so the provider can't
  # fetch an account key to manage the checkpoint container - it uses Entra ID
  # instead.
  storage_use_azuread = true
}

module "app-service-eventhub" {
  source = "../../modules/app-service-eventhub"

  name_prefix = "locally-example-eventhub"
  location    = "berlin"
  tags = {
    ProvisionedVia = "Terraform"
  }
}
