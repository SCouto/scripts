#!/usr/bin/env python3

import boto3
import subprocess
import sys
import re
from typing import List, Tuple

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

def get_bucket_size_cli(bucket_name: str) -> Tuple[str, int]:
    """
    Obtiene el tamaño de un bucket usando AWS CLI.

    :param bucket_name: Nombre del bucket
    :return: Tuple (bucket_name, size_in_bytes)
    """
    try:
        # Ejecutar aws s3 ls con --summarize para obtener el tamaño total
        cmd = ['aws', 's3', 'ls', f's3://{bucket_name}/', '--recursive', '--summarize']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5000)

        if result.returncode != 0:
            print(f"⚠️  Error accediendo al bucket '{bucket_name}': {result.stderr.strip()}")
            return bucket_name, 0

        # Buscar la línea "Total Size: X bytes"
        output = result.stdout
        size_match = re.search(r'Total Size:\s+(\d+)', output)

        if size_match:
            return bucket_name, int(size_match.group(1))
        else:
            # Si no hay match, probablemente el bucket está vacío
            return bucket_name, 0

    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout accediendo al bucket '{bucket_name}'")
        return bucket_name, 0
    except FileNotFoundError:
        print("❌ AWS CLI no está instalado. Por favor instala AWS CLI primero.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error procesando bucket '{bucket_name}': {e}")
        return bucket_name, 0

def check_aws_cli():
    """
    Verifica que AWS CLI esté instalado y configurado.
    """
    try:
        result = subprocess.run(['aws', '--version'], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ AWS CLI no está disponible")
            return False
        print(f"✅ {result.stdout.strip()}")
        return True
    except FileNotFoundError:
        print("❌ AWS CLI no está instalado. Por favor instala AWS CLI primero:")
        print("   • macOS: brew install awscli")
        print("   • Ubuntu/Debian: sudo apt install awscli")
        print("   • Windows: https://aws.amazon.com/cli/")
        return False

def format_size(size_bytes: int) -> str:
    """
    Formatea el tamaño en bytes a una representación legible.
    """
    if size_bytes == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB']
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
    print("🚀 S3 Bucket Size Analyzer (AWS CLI Version)")
    print("=" * 60)

    # Verificar AWS CLI
    if not check_aws_cli():
        sys.exit(1)

    # Obtener lista de buckets
    print("\n🔍 Obteniendo lista de buckets...")
    bucket_names = get_all_bucket_names()

    if not bucket_names:
        print("ℹ️  No se encontraron buckets.")
        return

    print(f"📦 Encontrados {len(bucket_names)} bucket(s)")
    print("\n📊 Calculando tamaños...")

    # Obtener tamaños usando AWS CLI
    bucket_sizes = []
    for i, bucket_name in enumerate(bucket_names, 1):
        print(f"  [{i}/{len(bucket_names)}] {bucket_name}...", end=" ", flush=True)
        name, size = get_bucket_size_cli(bucket_name)
        bucket_sizes.append((name, size))
        print(f"{format_size(size)}")

    # Ordenar por tamaño (de mayor a menor)
    bucket_sizes.sort(key=lambda x: x[1], reverse=True)

    # Mostrar resumen
    print("\n" + "="*60)
    print("📈 RESUMEN DE TAMAÑOS POR BUCKET")
    print("="*60)

    total_size = 0
    for bucket_name, size in bucket_sizes:
        total_size += size
        print(f"{bucket_name}: {format_size(size)}")

    print("-" * 60)
    print(f"🎯 TOTAL: {format_size(total_size)}")

if __name__ == '__main__':
    main()
