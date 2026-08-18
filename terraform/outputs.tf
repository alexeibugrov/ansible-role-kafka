output "kafka_nodes" {
  description = "Per-node connection details and the KRaft node ID."
  value = {
    for name, instance in aws_instance.kafka :
    name => {
      hostname          = name
      node_id           = tonumber(instance.tags["KafkaNodeId"])
      public_ip         = instance.public_ip
      private_ip        = instance.private_ip
      private_dns       = instance.private_dns
      availability_zone = instance.availability_zone
      instance_id       = instance.id
    }
  }
}

output "ssh_username" {
  description = "Login user for Ansible."
  value       = var.ssh_username
}

output "region" {
  description = "Region the test cluster runs in."
  value       = var.region
}

output "ssh_ingress_cidr" {
  description = "CIDR allowed to reach SSH, echoed back so the e2e script can assert it."
  value       = var.ssh_ingress_cidr
}

output "public_ips" {
  description = "Public addresses, used by the negative test that proves Kafka ports are closed."
  value       = [for instance in aws_instance.kafka : instance.public_ip]
}
