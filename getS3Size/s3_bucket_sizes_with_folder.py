#!/usr/bin/env python3

import boto3
import sys
import re
import argparse
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
            #if 'lifecycle' in bucket_name:
            #    continue

            # Skip buckets with 'logs' in name
            #if 'logs' in bucket_name:
            #    continue

            # Skip buckets that match 'db-XX' pattern (where XX is any characters)
            #if re.match(r'.*db-.*', bucket_name):
            #    continue

            filtered_buckets.append(bucket['Name'])

        return filtered_buckets
    except Exception as e:
        print(f"❌ Error listando buckets: {e}")
        return []

def get_folder_size_s3(bucket_name: str, folder_path: str) -> Tuple[str, int, int, dict]:
    """
    Calcula el tamaño de una carpeta específica en un bucket S3 y agrupa por subcarspetas.

    :param bucket_name: Nombre del bucket
    :param folder_path: Ruta de la carpeta (ej: "data-realestate-lifecycle-pro/user_model")
    :return: Tuple (folder_path, size_in_bytes, object_count, subfolder_sizes)
    """
    try:
        s3 = boto3.client('s3')

        # Asegurar que el path termine con / si es una carpeta
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'

        total_size = 0
        object_count = 0
        subfolder_sizes = {}  # Diccionario para almacenar tamaños por subcarpeta

        # Usar paginador para manejar respuestas grandes
        paginator = s3.get_paginator('list_objects_v2')

        # Configurar parámetros de la consulta
        params = {
            'Bucket': bucket_name,
            'Prefix': folder_path
        }

        print(f"🔍 Analizando objetos en: s3://{bucket_name}/{folder_path}")

        # Iterar a través de todas las páginas
        for page in paginator.paginate(**params):
            if 'Contents' in page:
                for obj in page['Contents']:
                    obj_key = obj['Key']
                    obj_size = obj['Size']

                    total_size += obj_size
                    object_count += 1

                    # Determinar la subcarpeta inmediata
                    # Remover el prefijo de la carpeta base
                    relative_path = obj_key[len(folder_path):]

                    if '/' in relative_path:
                        # Es un archivo dentro de una subcarpeta
                        subfolder = relative_path.split('/')[0]
                        subfolder_key = f"{folder_path}{subfolder}/"
                    else:
                        # Es un archivo directamente en la carpeta base
                        subfolder_key = folder_path + "(archivos directos)"

                    # Agregar al diccionario de subcarpetas
                    if subfolder_key not in subfolder_sizes:
                        subfolder_sizes[subfolder_key] = {'size': 0, 'count': 0}

                    subfolder_sizes[subfolder_key]['size'] += obj_size
                    subfolder_sizes[subfolder_key]['count'] += 1

                    # Mostrar progreso cada 1000 objetos
                    if object_count % 1000 == 0:
                        print(f"  📊 Procesados {object_count} objetos, tamaño acumulado: {format_size(total_size)}")

        return folder_path, total_size, object_count, subfolder_sizes

    except Exception as e:
        print(f"❌ Error calculando tamaño de carpeta '{folder_path}' en bucket '{bucket_name}': {e}")
        return folder_path, 0, 0, {}

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

def parse_folder_path(folder_arg: str) -> Tuple[str, str]:
    """
    Parsea el argumento de carpeta para extraer bucket y path.
    Formatos soportados:
    - bucket-name/folder/path
    - s3://bucket-name/folder/path
    """
    if folder_arg.startswith('s3://'):
        # Remover prefijo s3://
        folder_arg = folder_arg[5:]

    # Dividir en bucket y path
    parts = folder_arg.split('/', 1)
    if len(parts) == 1:
        # Solo bucket, sin carpeta específica
        return parts[0], ""
    else:
        return parts[0], parts[1]

def main():
    parser = argparse.ArgumentParser(
        description='🚀 S3 Bucket Size Analyzer - Analiza buckets completos o carpetas específicas',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Ejemplos de uso:
  %(prog)s                                    # Analizar todos los buckets
  %(prog)s --folder bucket-name/folder/path   # Analizar carpeta específica
  %(prog)s --folder s3://bucket-name/folder   # Usar formato S3 URI
  %(prog)s --folder data-realestate-lifecycle-pro/user_model  # Tu ejemplo
        '''
    )

    parser.add_argument(
        '--folder',
        type=str,
        help='Ruta específica de carpeta a analizar (formato: bucket-name/folder/path o s3://bucket-name/folder/path)'
    )

    parser.add_argument(
        '--max-subfolders',
        type=int,
        default=50,
        help='Número máximo de subcarpetas a mostrar en el desglose (por defecto: 50)'
    )

    args = parser.parse_args()

    print("🚀 S3 Bucket Size Analyzer")
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

    # Modo carpeta específica
    if args.folder:
        print(f"\n📁 Modo: Análisis de carpeta específica")
        bucket_name, folder_path = parse_folder_path(args.folder)

        print(f"🎯 Bucket: {bucket_name}")
        print(f"📂 Carpeta: {folder_path if folder_path else '(raíz)'}")
        print("\n🔍 Calculando tamaño de carpeta...")
        print("⚠️  Nota: Este método enumera todos los objetos y puede tardar en carpetas grandes")

        folder_path_result, total_size, object_count, subfolder_sizes = get_folder_size_s3(bucket_name, folder_path)

        print("\n" + "="*70)
        print("📊 RESULTADO DEL ANÁLISIS DE CARPETA")
        print("="*70)
        print(f"📦 Bucket: {bucket_name}")
        print(f"📁 Carpeta: {folder_path if folder_path else '(raíz)'}")
        print(f"📏 Tamaño total: {format_size(total_size)}")
        print(f"🔢 Número de objetos: {object_count:,}")
        if object_count > 0:
            avg_size = total_size / object_count
            print(f"📊 Tamaño promedio por objeto: {format_size(int(avg_size))}")

        # Mostrar desglose por subcarpetas si hay datos
        if subfolder_sizes:
            print("\n" + "-"*70)
            print("📂 DESGLOSE POR SUBCARPETAS:")
            print("-"*70)

            # Ordenar subcarpetas por tamaño (mayor a menor)
            sorted_subfolders = sorted(
                subfolder_sizes.items(),
                key=lambda x: x[1]['size'],
                reverse=True
            )

            # Mostrar solo las N subcarpetas más grandes según el límite
            displayed_count = 0
            remaining_size = 0
            remaining_count = 0

            for subfolder_path, data in sorted_subfolders:
                if displayed_count < args.max_subfolders:
                    size = data['size']
                    count = data['count']
                    percentage = (size / total_size * 100) if total_size > 0 else 0

                    # Formatear el nombre de la subcarpeta para mostrar
                    display_name = subfolder_path
                    if display_name.startswith(folder_path):
                        display_name = display_name[len(folder_path):]
                    if display_name.endswith('/'):
                        display_name = display_name[:-1]
                    if not display_name:
                        display_name = "(archivos directos)"

                    print(f"  📁 {display_name:<40} : {format_size(size):>12} ({percentage:5.1f}%) - {count:,} objetos")
                    displayed_count += 1
                else:
                    # Acumular las subcarpetas restantes
                    remaining_size += data['size']
                    remaining_count += data['count']

            # Mostrar resumen de subcarpetas restantes si las hay
            if remaining_size > 0:
                remaining_folders = len(sorted_subfolders) - args.max_subfolders
                percentage = (remaining_size / total_size * 100) if total_size > 0 else 0
                print(f"  📁 {f'... y {remaining_folders} subcarpetas más':<40} : {format_size(remaining_size):>12} ({percentage:5.1f}%) - {remaining_count:,} objetos")

        print("="*70)

        return

    # Modo análisis completo de buckets (funcionalidad original)
    print(f"\n📦 Modo: Análisis completo de buckets")

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