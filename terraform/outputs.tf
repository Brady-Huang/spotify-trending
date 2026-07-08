output "vm_ip" {
  value = google_compute_address.static_ip.address
}

output "api_url" {
  value = "http://${google_compute_address.static_ip.address}:8000"
}

output "spark_ui_url" {
  value = "http://${google_compute_address.static_ip.address}:8080"
}