def transform_weather(**context):
    ti = context["ti"]

    data = ti.xcom_pull(task_ids="extract")

    transformed = {
        "city": data["city"].strip().title(),
        "temperature": float(data["temperature"]),
        "collected_at": data["collected_at"]
    }

    return transformed
