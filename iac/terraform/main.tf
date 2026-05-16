terraform {
  required_version = ">= 1.5"
  required_providers {
    aws       = { source = "hashicorp/aws",       version = "~> 5.0" }
    azurerm   = { source = "hashicorp/azurerm",   version = "~> 3.0" }
    google    = { source = "hashicorp/google",    version = "~> 5.0" }
    snowflake = { source = "Snowflake-Labs/snowflake", version = "~> 0.90" }
  }
}

provider "aws"     { region = "us-east-1" }
provider "azurerm" { features {} }
provider "google"  { project = var.gcp_project }
provider "snowflake" {}

variable "gcp_project" { default = "hmis-platform" }

# --- AWS --------------------------------------------------------------------
resource "aws_s3_bucket" "lake"    { bucket = "hmis-lake" }
resource "aws_s3_bucket" "landing" { bucket = "hmis-landing" }

resource "aws_glue_catalog_database" "hmis_db" { name = "hmis_db" }

resource "aws_redshiftserverless_namespace" "ns" {
  namespace_name      = "hmis-ns"
  admin_username      = "admin"
  admin_user_password = "REPLACE_ME"
}
resource "aws_redshiftserverless_workgroup" "wg" {
  namespace_name = aws_redshiftserverless_namespace.ns.namespace_name
  workgroup_name = "hmis-wg"
  base_capacity  = 8
}

# --- Azure ------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = "hmis-rg"
  location = "eastus2"
}
resource "azurerm_storage_account" "adls" {
  name                     = "hmislake"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" { name = "bronze" storage_account_id = azurerm_storage_account.adls.id }
resource "azurerm_storage_data_lake_gen2_filesystem" "silver" { name = "silver" storage_account_id = azurerm_storage_account.adls.id }
resource "azurerm_storage_data_lake_gen2_filesystem" "gold"   { name = "gold"   storage_account_id = azurerm_storage_account.adls.id }

# --- GCP --------------------------------------------------------------------
resource "google_bigquery_dataset" "raw"     { dataset_id = "hmis_raw"     location = "US" }
resource "google_bigquery_dataset" "curated" { dataset_id = "hmis_curated" location = "US" }
resource "google_pubsub_topic"        "events"  { name = "hmis-events" }
resource "google_pubsub_subscription" "events"  { name = "hmis-events-sub" topic = google_pubsub_topic.events.name }

# --- Snowflake --------------------------------------------------------------
resource "snowflake_database"  "hmis" { name = "HMIS" }
resource "snowflake_warehouse" "wh_reporting" {
  name           = "WH_REPORTING"
  warehouse_size = "SMALL"
  auto_suspend   = 60
  auto_resume    = true
}
