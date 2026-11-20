from pipeline_datos import pipeline_datos

pipeline_datos.serve(
    name="pipeline-ventas-superdata",
    interval=60
)
