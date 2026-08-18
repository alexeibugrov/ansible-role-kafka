resource "aws_security_group" "kafka" {
  name        = "${var.project_name}-sg"
  description = "Kafka test nodes: SSH ingress, Kafka ports intra-cluster only."
  vpc_id      = aws_vpc.kafka.id

  tags = {
    Name = "${var.project_name}-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.kafka.id
  description       = "SSH for the machine running Ansible"
  cidr_ipv4         = var.ssh_ingress_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_client" {
  security_group_id            = aws_security_group.kafka.id
  description                  = "Kafka BROKER listener (SASL_SSL), cluster-internal only"
  referenced_security_group_id = aws_security_group.kafka.id
  from_port                    = var.kafka_client_port
  to_port                      = var.kafka_client_port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_controller" {
  security_group_id            = aws_security_group.kafka.id
  description                  = "KRaft CONTROLLER listener (SSL/mTLS), cluster-internal only"
  referenced_security_group_id = aws_security_group.kafka.id
  from_port                    = var.kafka_controller_port
  to_port                      = var.kafka_controller_port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "kafka_metrics" {
  security_group_id            = aws_security_group.kafka.id
  description                  = "Prometheus JMX Exporter, scraped from inside the VPC only"
  referenced_security_group_id = aws_security_group.kafka.id
  from_port                    = var.kafka_metrics_port
  to_port                      = var.kafka_metrics_port
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.kafka.id
  description       = "Outbound to package mirrors and Apache/Maven download sources"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
