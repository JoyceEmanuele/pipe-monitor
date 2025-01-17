import json
import json5
import sys
from pathlib import Path

# Caminhos para os arquivos
example_file = Path(sys.argv[1])  # configfile_example.json5
configmap_file = Path(sys.argv[2])  # configfile.yaml

# Carregar o configfile_example.json5 usando json5
with example_file.open() as f:
    example_config = json5.load(f)

# Extrair o JSON do configfile.yaml
with configmap_file.open() as f:
    lines = f.readlines()

# Identificar a seção que contém o JSON no configfile.yaml
start_index = next(i for i, line in enumerate(lines) if line.strip() == 'configfile.json5: |')
json_config = '\n'.join(line[4:] for line in lines[start_index + 1:])

# Carregar o JSON do configfile.yaml usando json5
current_config = json5.loads(json_config)

# Mesclar os arquivos
merged_config = {**example_config, **current_config}

# Atualizar o configfile.yaml com o JSON mesclado
merged_json = json.dumps(merged_config, indent=2)
lines[start_index + 1:] = ['    ' + line + '\n' for line in merged_json.splitlines()]

# Salvar o configfile.yaml atualizado
with configmap_file.open("w") as f:
    f.writelines(lines)
