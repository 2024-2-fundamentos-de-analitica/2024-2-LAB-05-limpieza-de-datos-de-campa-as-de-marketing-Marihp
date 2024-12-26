"""
Escriba el codigo que ejecute la accion solicitada.
"""

import pandas as pd

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    # Directorios de entrada y salida como cadenas
    input_path = "files/input/"
    output_path = "files/output/"

    # Procesar cada archivo ZIP
    zip_files = [
        f"{input_path}{file}"
        for file in pd.io.common.os.listdir(input_path)
        if file.endswith(".zip")
    ]

    # Leer cada archivo y concatenar en un df
    data = pd.concat(
        [pd.read_csv(file) for file in zip_files],
        ignore_index=True,
    )
    # print(data.info())

    # Procesar datos
    client = data[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ].copy()

    # print(client.info())

    # Transformaciones para client.csv
    client["job"] = client["job"].str.replace(".", "").str.replace("-", "_")
    client["education"] = (
        client["education"].str.replace(".", "_").replace("unknown", pd.NA)
    )
    client["credit_default"] = client["credit_default"].apply(
        lambda x: 1 if x == "yes" else 0
    )
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    # print(client)

    client.to_csv(output_path + "client.csv", index=False)

    # CAMPAIGN.CSV -------------------------------------------------------

    campaign = data[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()

    # Transformaciones para campaign.csv
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(
        lambda x: 1 if x == "success" else 0
    )
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(
        lambda x: 1 if x == "yes" else 0
    )
    campaign["last_contact_date"] = pd.to_datetime(
        "2022-" + campaign["month"] + "-" + campaign["day"].astype(str),
        format="%Y-%b-%d",
    )

    # Seleccionar columnas finales y guardar
    campaign = campaign[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_outcome",
            "previous_campaign_contacts",
            "campaign_outcome",
            "last_contact_date",
        ]
    ]
    campaign.to_csv(output_path + "campaign.csv", index=False)

    # ECONOMICS.CSV ---------------------------------
    economics = data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Guardar economics.csv
    economics.to_csv(output_path + "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
