import boto3
import sys
import re
from collections import defaultdict

def get_s3_folder_sizes(bucket_name: str, prefix: str = '') -> dict:
    """
    Devuelve un dict con el tamaño (en bytes) de cada carpeta de primer nivel dentro del prefijo dado.

    :param bucket_name: Nombre del bucket
    :param prefix: Prefijo (opcional) para acotar la búsqueda
    :return: Diccionario {carpeta_de_primer_nivel: tamaño_en_bytes}
    """
    s3 = boto3.client('s3')
    folder_sizes = defaultdict(int)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            size = obj['Size']
            stripped_key = key[len(prefix):] if prefix else key
            parts = stripped_key.split('/')
            folder = parts[0] if '/' in stripped_key else '(root)'
            folder_sizes[folder] += size

    return dict(folder_sizes)

def parse_bucket_and_prefix(arg: str) -> tuple[str, str]:
    """Parses input like 'my-bucket/path/to/prefix' into bucket and prefix"""
    parts = arg.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] + '/' if len(parts) > 1 else ''
    return bucket, prefix

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python3 s3_folder_sizes.py <bucket>[/optional/prefix]")
        sys.exit(1)

    bucket, prefix = parse_bucket_and_prefix(sys.argv[1])

    # Validación opcional
    BUCKET_REGEX = r'^[a-z0-9.\-_]{3,63}$'
    if not re.match(BUCKET_REGEX, bucket):
        print(f"❌ Bucket name '{bucket}' is invalid.")
        sys.exit(1)

    print(f"🔍 Analizando bucket '{bucket}' con prefijo '{prefix}'...")

    sizes = get_s3_folder_sizes(bucket, prefix)

    if not sizes:
        print("⚠️  No se encontraron objetos.")
    else:
        for folder, total_size in sorted(sizes.items(), key=lambda x: x[1], reverse=True):
            size_mb = total_size / (1024 * 1024)
            print(f'{folder}: {size_mb:.2f} MB')

