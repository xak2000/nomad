job "binstore-storagelocker" {
  group "binsl" {
    task "binstore" {
      driver = "docker"

      csi_plugin {
        plugin_id        = "org.hashicorp.csi"
        plugin_type      = "monolith"
        plugin_mount_dir = "/csi/test"
      }
    }
  }
}
