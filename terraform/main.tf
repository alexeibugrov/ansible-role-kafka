data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_key_pair" "kafka" {
  key_name   = "${var.project_name}-key"
  public_key = trimspace(file(var.ssh_public_key_path))

  tags = {
    Name = "${var.project_name}-key"
  }
}

locals {
  nodes = {
    for i in range(var.node_count) :
    format("kafka-%d", i + 1) => {
      node_id = i + 1
    }
  }
}

resource "aws_instance" "kafka" {
  for_each = local.nodes

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.kafka.id
  vpc_security_group_ids      = [aws_security_group.kafka.id]
  key_name                    = aws_key_pair.kafka.key_name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = var.root_volume_size
    volume_type           = var.root_volume_type
    encrypted             = true
    delete_on_termination = true

    tags = {
      Name = "${var.project_name}-${each.key}-root"
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  user_data = <<-EOT
    preserve_hostname: false
    hostname: ${each.key}
    fqdn: ${each.key}
  EOT

  tags = {
    Name        = "${var.project_name}-${each.key}"
    KafkaNodeId = tostring(each.value.node_id)
  }
}
