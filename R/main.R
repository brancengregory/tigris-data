library(arcgis)
library(httr2)
library(yyjsonr)
library(purrr)
library(dplyr)
library(stringr)
library(janitor)
library(fs)

base_url <- "https://tigerweb.geo.census.gov/arcgis/rest/services/"

req_request_json <- function(req) {
  req |> req_url_query("f" = "pjson")
}

traverse_services <- function(base_url = base_url) {
  folders <- get_folders(base_url)
  traverse_folders(folders, base_url)
}

get_folders <- function(base_url) {
  res <- request(base_url) |>
    req_request_json() |>
    req_perform() |>
    resp_body_string() |>
    yyjsonr::read_json_str()
  res$folders
}

traverse_folders <- function(folders, base_url) {
  map_dfr(folders, function(x) {
    res <- request(base_url) |>
      req_url_path_append(x) |>
      req_request_json() |>
      req_perform() |>
      resp_body_string() |>
      yyjsonr::read_json_str()
    res$services
  }) |>
    rename(service = name)
}

get_service <- function(service, type, base_url) {
  res <- request(base_url) |>
    req_url_path_append(service, type) |>
    req_request_json() |>
    req_perform() |>
    resp_body_string() |>
    yyjsonr::read_json_str()
  res
}

get_services <- function(services, base_url) {
  services |>
    mutate(base_url = base_url) |>
    pmap(get_service)
}

open_service <- function(service, type, base_url) {
  url <- str_c(base_url, service, "/", type)
  url <- url_modify_query(url, "f" = "pjson")
  arcgislayers::arc_open(url)
}

process_layer <- function(layer, service, output_dir) {
  if (inherits(layer, "GroupLayer")) {
    sublayers <- arcgislayers::get_all_layers(layer)$layers
    walk(sublayers, process_layer, service, output_dir)
  } else if (inherits(layer, "FeatureLayer") || inherits(layer, "Table")) {
    data <- tryCatch(
      arcgislayers::arc_select(layer),
      error = function(e) {
        warning(glue::glue("Failed to query layer {layer$name}: {e$message}"))
        NULL
      }
    )
    if (!is.null(data)) {
      clean_layer_name <- make_clean_names(layer$name)
      service_dir <- path(output_dir, service)
      dir_create(service_dir)
      output_path <- path(service_dir, str_c(clean_layer_name, ".geojson"))
      write_geojson_file(data, output_path)
    }
  } else if (inherits(layer, "ImageServer")) {
    raster_data <- tryCatch(
      arcgislayers::arc_raster(layer),
      error = function(e) {
        warning(glue::glue("Failed to query ImageServer layer {layer$name}: {e$message}"))
        NULL
      }
    )
    if (!is.null(raster_data)) {
      clean_layer_name <- make_clean_names(layer$name)
      service_dir <- path(output_dir, service)
      dir_create(service_dir)
      output_path <- path(service_dir, str_c(clean_layer_name, ".tif"))
      terra::writeRaster(raster_data, output_path, format = "GTiff")
    }
  } else {
    message(glue::glue("Skipping unsupported layer type: {layer$name}"))
  }
}

scrape_geodata <- function(services, base_url, output_dir) {
  purrr::walk2(
    services$service,
    services$type,
    function(service, type) {
      # Open the service
      arc_obj <- tryCatch(
        open_service(service, type, base_url),
        error = function(e) {
          warning(glue::glue("Failed to open service {service}/{type}: {e$message}"))
          return(NULL)
        }
      )
      if (is.null(arc_obj)) return(NULL)

      # Get all layers
      layers_list <- arcgislayers::get_all_layers(arc_obj)$layers

      # Process each layer with a progress bar
      purrr::walk(
        layers_list,
        process_layer,
        service,
        output_dir,
        .progress = TRUE
      )
    },
    .progress = TRUE
  )
}


services <- traverse_services(base_url)
service_details <- get_services(services, base_url)

output_dir <- "./data"

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

data <- scrape_geodata(services, base_url, output_dir)

# s <- services[1,]
# ms <- s |>
#   mutate(base_url = base_url) |>
#   pmap(open_service, .progress = TRUE) |>
#   pluck(1)
# layers_list <- arcgislayers::get_all_layers(ms)$layers

# all_data <- layers_list |>
#   purrr::map(~ {
#     tryCatch(
#       arcgislayers::arc_select(.x),
#       error = \(e) {
#         warning(str_c("Failed to query layer ", .x$name, ": ", e$message))
#         NULL
#       }
#     )
#   },
#   .progress = TRUE
# )
