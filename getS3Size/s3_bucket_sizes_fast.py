#!/usr/bin/env python3

import boto3
import sys
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_all_bucket_names() -> List[str]:
    """
    Obtiene la lista de todos los buckets usando boto3.
    Excluye buckets que contienen 'lifecycle', 'logs' en su nombre o que siguen el patrón 'db-XX'.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()

        filtered_buckets = []
        for bucket in response['Buckets']:
            bucket_name = bucket['Name'].lower()

            # Skip buckets with 'lifecycle' in name
            if 'lifecycle' in bucket_name:
                continue

            # Skip buckets with 'logs' in name
            if 'logs' in bucket_name:
                continue

            # Skip buckets that match 'db-XX' pattern (where XX is any characters)
            if re.match(r'.*db-.*', bucket_name):
                continue

            filtered_buckets.append(bucket['Name'])

        return filtered_buckets
    except Exception as e:
        print(f"❌ Error listando buckets: {e}")
        return []

def get_bucket_size_cloudwatch(bucket_name: str) -> Tuple[str, int]:
    """
    Obtiene el tamaño de un bucket usando métricas de CloudWatch.
    Esto es MUCHO más rápido que enumerar todos los objetos.

    :param bucket_name: Nombre del bucket
    :return: Tuple (bucket_name, size_in_bytes)
    """
    try:
        cloudwatch = boto3.client('cloudwatch')

        # Buscar métricas de los últimos 2 días
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=2)

        # Obtener métricas de tamaño para StandardStorage
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='BucketSizeBytes',
            Dimensions=[
                {'Name': 'BucketName', 'Value': bucket_name},
                {'Name': 'StorageType', 'Value': 'StandardStorage'}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,  # 1 día
            Statistics=['Average']
        )

        if response['Datapoints']:
            # Obtener el valor más reciente
            latest = max(response['Datapoints'], key=lambda x: x['Timestamp'])
            return bucket_name, int(latest['Average'])
        else:
            # Intentar con todos los tipos de almacenamiento
            storage_classes = ['StandardStorage', 'StandardIAStorage', 'ReducedRedundancyStorage',
                             'GlacierStorage', 'DeepArchiveStorage', 'IntelligentTieringFAStorage',
                             'IntelligentTieringIAStorage', 'IntelligentTieringAAStorage',
                             'IntelligentTieringAIAStorage', 'IntelligentTieringDAAStorage']

            total_size = 0
            for storage_class in storage_classes:
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': storage_class}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=['Average']
                )

                if response['Datapoints']:
                    latest = max(response['Datapoints'], key=lambda x: x['Timestamp'])
                    total_size += int(latest['Average'])

            return bucket_name, total_size

    except Exception as e:
        print(f"⚠️  Error obteniendo métricas para '{bucket_name}': {e}")
        return bucket_name, 0

def get_bucket_region(bucket_name: str) -> Optional[str]:
    """
    Obtiene la región de un bucket.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.get_bucket_location(Bucket=bucket_name)
        region = response['LocationConstraint']
        return region if region else 'us-east-1'  # us-east-1 devuelve None
    except Exception as e:
        print(f"⚠️  Error obteniendo región para '{bucket_name}': {e}")
        return None

def process_bucket_with_progress(args):
    """
    Procesa un bucket y muestra progreso thread-safe.
    """
    bucket_name, bucket_index, total_buckets, lock = args

    name, size = get_bucket_size_cloudwatch(bucket_name)

    with lock:
        print(f"  [{bucket_index}/{total_buckets}] {bucket_name}: {format_size(size)}")

    return name, size

def format_size(size_bytes: int) -> str:
    """
    Formatea el tamaño en bytes a una representación legible.
    """
    if size_bytes == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(size_bytes)
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"

def main():
    print("🚀 S3 Bucket Size Analyzer (CloudWatch Version - FAST!)")
    print("=" * 70)

    # Verificar credenciales AWS
    try:
        boto3.client('sts').get_caller_identity()
        print("✅ Credenciales AWS verificadas")
    except Exception as e:
        print(f"❌ Error de credenciales AWS: {e}")
        print("Asegúrate de tener configuradas las credenciales AWS:")
        print("  • aws configure")
        print("  • Variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY")
        print("  • Perfil IAM en EC2")
        sys.exit(1)

    # Obtener lista de buckets
    print("\n🔍 Obteniendo lista de buckets...")
    bucket_names = get_all_bucket_names()

    if not bucket_names:
        print("ℹ️  No se encontraron buckets.")
        return

    print(f"📦 Encontrados {len(bucket_names)} bucket(s)")
    print("\n📊 Calculando tamaños usando CloudWatch (paralelo)...")
    print("ℹ️  Nota: Los tamaños se actualizan diariamente en CloudWatch")

    # Procesar buckets en paralelo
    bucket_sizes = []
    lock = threading.Lock()

    # Preparar argumentos para el procesamiento paralelo
    args_list = [
        (bucket_name, i + 1, len(bucket_names), lock)
        for i, bucket_name in enumerate(bucket_names)
    ]

    # Usar ThreadPoolExecutor para procesar múltiples buckets en paralelo
    max_workers = min(20, len(bucket_names))  # Máximo 20 threads concurrentes
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todas las tareas
        future_to_bucket = {
            executor.submit(process_bucket_with_progress, args): args[0]
            for args in args_list
        }

        # Recoger resultados conforme se completan
        for future in as_completed(future_to_bucket):
            bucket_name = future_to_bucket[future]
            try:
                name, size = future.result()
                bucket_sizes.append((name, size))
            except Exception as e:
                print(f"❌ Error procesando {bucket_name}: {e}")
                bucket_sizes.append((bucket_name, 0))

    # Ordenar por tamaño (de mayor a menor)
    bucket_sizes.sort(key=lambda x: x[1], reverse=True)

    # Mostrar resumen
    print("\n" + "="*70)
    print("📈 RESUMEN DE TAMAÑOS POR BUCKET")
    print("="*70)

    total_size = 0
    for bucket_name, size in bucket_sizes:
        total_size += size
        print(f"{bucket_name:40} : {format_size(size):>15}")

    print("-" * 70)
    print(f"🎯 TOTAL: {format_size(total_size)}")
    print(f"\n⚡ Procesamiento completado usando CloudWatch metrics!")
    print("ℹ️  Los datos son de las últimas 24-48 horas")

if __name__ == '__main__':
    main()