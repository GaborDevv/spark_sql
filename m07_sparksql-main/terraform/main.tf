# Setup azurerm as a state backend
terraform {
  backend "azurerm" {
	resource_group_name   = "spark-basics"
    	storage_account_name  = "gaborsstorage"
    	container_name        = "new"
    	key                   = "prod.terraform.tfstate"
	access_key 	      ="32Dmym0hdFxTYgwvHK5M2AsapmfkYrPe3JQpRXwMEhz5dHe73kyuOusVoyzXhO7dH4WraEZjenYo+AStnenrgA=="
	}
}

# Configure the Microsoft Azure Provider
provider "azurerm" {
  features {}
}



data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "bdcc" {
  name = "rg-${var.ENV}-${var.LOCATION}"
  location = var.LOCATION

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc]

  name = "st${var.ENV}${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  account_tier = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled = "true"

  network_rules {
    default_action = "Allow"
    ip_rules = values(var.IP_RULES)
  }

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gen2_data" {
  depends_on = [
    azurerm_storage_account.bdcc]

  name = "data"
  storage_account_id = azurerm_storage_account.bdcc.id

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name = "dbw-${var.ENV}-${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location = azurerm_resource_group.bdcc.location
  sku = "standard"

  tags = {
    region = var.BDCC_REGION
    env = var.ENV
  }
}

provider "databricks" {
  host  = "https://${azurerm_databricks_workspace.bdcc.workspace_url}"
  token = var.databricks_token
}

resource "databricks_job" "bdcc1" {
 name = "SPARKSQL_TERRAFORM"
 
 new_cluster {
 
 spark_version = "13.3.x-scala2.12"
 
   
  azure_attributes {
   first_on_demand= 1
   availability = "ON_DEMAND_AZURE"
   spot_bid_max_price = -1
 }
 
  node_type_id = "Standard_DS3_v2"
  enable_elastic_disk = true 
  
  spark_conf= {
        "spark.master": "local[*]"
        "spark.databricks.cluster.profile": "singleNode"
    }
    
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

 }
 
 notebook_task{
  notebook_path = "/Repos/gabor_rabai@epam.com/m07_sparksql/src/main/python/main"
 }
 
}

output "job_url"{
 value = databricks_job.bdcc1.url
}