import boto3
import uuid
import os
import json


def lambda_handler(event, context):
    # Entrada
    print(event)
    tenant_id = event["body"]["tenant_id"]
    texto = event["body"]["texto"]
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_ingesta = os.environ["BUCKET_INGESTA"]

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {"tenant_id": tenant_id, "uuid": uuidv1, "detalle": {"texto": texto}}

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Guardar JSON en el bucket de ingesta (estrategia PUSH)
    s3 = boto3.client("s3")
    archivo_key = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=bucket_ingesta,
        Key=archivo_key,
        Body=json.dumps(comentario),
        ContentType="application/json",
    )

    # Salida
    return {
        "statusCode": 200,
        "comentario": comentario,
        "s3_key": archivo_key,
        "response": response,
    }
