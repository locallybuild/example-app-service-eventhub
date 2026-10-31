locals {
  # The application image, built from ../../app and pushed into the Locally
  # Container Registry by 5-container-registry.tf.
  image_name = "app-service-eventhub"

  # The image is tagged with a hash of the application directory, so any change
  # to the app produces a new tag: that re-triggers the build and push, and rolls
  # both App Services onto the freshly built image. Build artifacts are excluded
  # so they don't churn the hash.
  app_dir = "${path.module}/../../app"
  app_files = [
    for f in fileset(local.app_dir, "**") : f
    if !startswith(f, "node_modules/") && !startswith(f, "dist/")
  ]
  image_tag = substr(sha1(join(",", [for f in local.app_files : "${f}:${filesha1("${local.app_dir}/${f}")}"])), 0, 12)

  # The port the container listens on inside App Service (the app's PORT, exposed
  # to App Service as WEBSITES_PORT). App Service terminates TLS at the platform
  # and routes public traffic to the container on this port.
  app_port = 8080

  # The namespace's hostname, which the Event Hubs clients connect to over AMQP.
  # azurerm_eventhub_namespace doesn't export the endpoint on its own, and the
  # hostname isn't always <name>.servicebus.windows.net (sovereign clouds, and
  # Locally, use other domains), so it's read from the Endpoint=sb://<host>/
  # part of the namespace's connection string. Only the hostname is used - the
  # key in that string is useless anyway, as local authentication is disabled.
  eventhub_hostname = nonsensitive(regex("Endpoint=sb://([^/;]+)", azurerm_eventhub_namespace.main.default_primary_connection_string)[0])
}
