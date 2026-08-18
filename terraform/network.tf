data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "kafka" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

resource "aws_internet_gateway" "kafka" {
  vpc_id = aws_vpc.kafka.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

resource "aws_subnet" "kafka" {
  vpc_id                  = aws_vpc.kafka.id
  cidr_block              = var.subnet_cidr
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.project_name}-subnet"
  }
}

resource "aws_route_table" "kafka" {
  vpc_id = aws_vpc.kafka.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.kafka.id
  }

  tags = {
    Name = "${var.project_name}-rt"
  }
}

resource "aws_route_table_association" "kafka" {
  subnet_id      = aws_subnet.kafka.id
  route_table_id = aws_route_table.kafka.id
}
