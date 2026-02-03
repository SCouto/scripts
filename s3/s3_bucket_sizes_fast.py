#!/usr/bin/env python3

import boto3
import sys
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_all_bucket_names() -> List[str]:
    """
    Obtiene la lista de todos los buckets usando boto3.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']]
    except Exception as e:
        print(f"❌ Error listando buckets: {e}")
        return []

def get_bucket_size_enumeration(bucket_name: str) -> Tuple[str, int]:
    """
    Obtiene el tamaño de un bucket enumerando todos los objetos.

    :param bucket_name: Nombre del bucket
    :return: Tuple (bucket_name, size_in_bytes)
    """
    try:
        s3 = boto3.client('s3')
        total_size = 0

        # Usar paginador para manejar respuestas grandes
        paginator = s3.get_paginator('list_objects_v2')
        params = {'Bucket': bucket_name}

        # Iterar a través de todas las páginas
        for page in paginator.paginate(**params):
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']

        return bucket_name, total_size

    except Exception as e:
        print(f"❌ Error enumerando objetos para '{bucket_name}': {e}")
        return bucket_name, 0

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

        # Lista de todas las clases de almacenamiento
        storage_classes = [
            'StandardStorage',
            'StandardIAStorage',
            'ReducedRedundancyStorage',
            'GlacierStorage',
            'DeepArchiveStorage',
            'IntelligentTieringFAStorage',
            'IntelligentTieringIAStorage',
            'IntelligentTieringAAStorage',
            'IntelligentTieringAIAStorage',
            'IntelligentTieringDAAStorage'
        ]

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

def get_bucket_size_by_subfolder(bucket_name: str, prefix: str = "") -> Dict[str, Dict[str, int]]:
    """
    Obtiene el tamaño agrupado por primera carpeta/subcarpeta después del prefijo.

    :param bucket_name: Nombre del bucket
    :param prefix: Prefijo opcional para filtrar objetos
    :return: Dict con {subfolder: {'size': size_in_bytes, 'count': object_count}}
    """
    try:
        s3 = boto3.client('s3')
        subfolder_data = {}
        total_objects_processed = 0
        total_size_processed = 0

        # Usar paginador para manejar respuestas grandes
        paginator = s3.get_paginator('list_objects_v2')

        # Configurar parámetros de la consulta
        params = {'Bucket': bucket_name}
        if prefix:
            params['Prefix'] = prefix

        # Iterar a través de todas las páginas
        for page in paginator.paginate(**params):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    size = obj['Size']

                    # Extraer la primera carpeta después del prefijo
                    # Remover el prefijo del key
                    relative_key = key[len(prefix):] if prefix else key

                    # Encontrar el primer '/' después del prefijo
                    slash_index = relative_key.find('/')

                    if slash_index != -1:
                        # Hay una subcarpeta - extraer el nombre de la primera subcarpeta
                        first_folder = relative_key[:slash_index + 1]
                        # Construir el path completo
                        full_path = prefix + first_folder
                    else:
                        # No hay subcarpeta - el archivo está directamente bajo el prefijo
                        full_path = prefix if prefix else "/"

                    # Acumular tamaño y contar objetos
                    if full_path not in subfolder_data:
                        subfolder_data[full_path] = {'size': 0, 'count': 0}

                    subfolder_data[full_path]['size'] += size
                    subfolder_data[full_path]['count'] += 1

                    total_objects_processed += 1
                    total_size_processed += size

                    # Mostrar progreso cada 10000 objetos
                    if total_objects_processed % 10000 == 0:
                        print(f"  Procesados {total_objects_processed:,} objetos, {format_size(total_size_processed)} acumulados...")

        # Mostrar total final si se procesaron objetos
        if total_objects_processed > 0:
            print(f"  ✓ Total procesados: {total_objects_processed:,} objetos, {format_size(total_size_processed)}")

        return subfolder_data

    except Exception as e:
        print(f"❌ Error enumerando objetos para '{bucket_name}': {e}")
        return {}

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
    parser = argparse.ArgumentParser(
        description='🚀 S3 Bucket Size Analyzer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ejemplos de uso:
  %(prog)s                                           # Listar tamaños de todos los buckets
  %(prog)s --bucket my-bucket-name                   # Analizar subcarpetas en un bucket
  %(prog)s --bucket my-bucket-name --prefix data/2024/  # Analizar subcarpetas con prefijo
        '''
    )

    parser.add_argument(
        '--bucket',
        help='Nombre del bucket a analizar (opcional, si no se especifica lista todos los buckets)'
    )

    parser.add_argument(
        '--prefix',
        default='',
        help='Prefijo opcional para filtrar objetos (solo válido con --bucket)'
    )

    args = parser.parse_args()

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

    # Modo 1: Análisis de subcarpetas de un bucket específico
    if args.bucket:
        print("\n🚀 S3 Bucket Size Analyzer - Subfolder Analysis")
        print("=" * 80)
        print(f"\n📦 Bucket: {args.bucket}")
        if args.prefix:
            print(f"📁 Prefix: {args.prefix}")
        else:
            print("📁 Prefix: (none - analyzing entire bucket)")

        print(f"\n📊 Analizando objetos y agrupando por primera subcarpeta...")

        # Obtener tamaños por subcarpeta
        subfolder_data = get_bucket_size_by_subfolder(args.bucket, args.prefix)

        if not subfolder_data:
            print("ℹ️  No se encontraron objetos.")
            return

        # Ordenar por tamaño (de mayor a menor)
        sorted_subfolders = sorted(
            subfolder_data.items(),
            key=lambda x: x[1]['size'],
            reverse=True
        )

        # Mostrar resumen
        print("\n" + "=" * 80)
        print("📈 TAMAÑOS POR SUBCARPETA (ordenado por tamaño)")
        print("=" * 80)
        print(f"{'Subfolder':<50} {'Size':>15} {'Objects':>10}")
        print("-" * 80)

        total_size = 0
        total_objects = 0
        for subfolder, data in sorted_subfolders:
            size = data['size']
            count = data['count']
            total_size += size
            total_objects += count
            print(f"{subfolder:<50} {format_size(size):>15} {count:>10,}")

        print("-" * 80)
        print(f"{'TOTAL':<50} {format_size(total_size):>15} {total_objects:>10,}")
        print(f"\n⚡ Análisis completado!")
        print(f"ℹ️  Se encontraron {len(sorted_subfolders)} subcarpeta(s)")

    # Modo 2: Listar tamaños de todos los buckets
    else:
        print("\n🚀 S3 Bucket Size Analyzer - All Buckets (CloudWatch)")
        print("=" * 70)

        # Obtener lista de buckets
        print("\n🔍 Obteniendo lista de buckets...")
        bucket_names = get_all_bucket_names()

        if not bucket_names:
            print("ℹ️  No se encontraron buckets.")
            return

        print(f"📦 Encontrados {len(bucket_names)} bucket(s)")
        print(f"\n📊 Calculando tamaños usando CloudWatch (paralelo)...")
        print(f"ℹ️  Nota: Los tamaños se actualizan diariamente en CloudWatch")

        # Procesar buckets en paralelo
        bucket_sizes = []
        lock = threading.Lock()

        def process_bucket(bucket_name, index, total):
            name, size = get_bucket_size_cloudwatch(bucket_name)
            with lock:
                print(f"  [{index}/{total}] {bucket_name}: {format_size(size)}")
            return name, size

        # Usar ThreadPoolExecutor para procesar múltiples buckets en paralelo
        max_workers = min(20, len(bucket_names))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_bucket, bucket_name, i + 1, len(bucket_names)): bucket_name
                for i, bucket_name in enumerate(bucket_names)
            }

            for future in as_completed(futures):
                try:
                    name, size = future.result()
                    bucket_sizes.append((name, size))
                except Exception as e:
                    bucket_name = futures[future]
                    print(f"❌ Error procesando {bucket_name}: {e}")
                    bucket_sizes.append((bucket_name, 0))

        # Ordenar por tamaño (de mayor a menor)
        bucket_sizes.sort(key=lambda x: x[1], reverse=True)

        # Mostrar resumen
        print("\n" + "=" * 70)
        print("📈 RESUMEN DE TAMAÑOS POR BUCKET")
        print("=" * 70)

        total_size = 0
        for bucket_name, size in bucket_sizes:
            total_size += size
            print(f"{bucket_name:40} : {format_size(size):>15}")

        print("-" * 70)
        print(f"🎯 TOTAL: {format_size(total_size)}")
        print(f"\n⚡ Procesamiento completado!")
        print(f"ℹ️  Los datos son de las últimas 24-48 horas (CloudWatch)")

if __name__ == '__main__':
    main()
