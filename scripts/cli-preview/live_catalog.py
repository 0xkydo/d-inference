"""Read-only public catalog, local inventory, and conservative native-policy estimates."""

import json
from pathlib import Path
import subprocess
import tempfile
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
URL = "https://api.darkbloom.dev/v1/models/catalog?type=text&include_aliases=1"


def clean(value):
    return "".join(c for c in str(value) if c.isprintable())[:180]


def public_models(catalog):
    """Match the production picker's public alias and hidden-build presentation."""
    names, hidden = {}, set()
    ids = {m['id'] for m in catalog['models']}
    for alias in catalog.get('aliases', []):
        builds = [alias.get('desired_build'), alias.get('previous_build')]
        primary = next((b for b in [alias.get('primary_build'), *builds] if b in ids), None)
        hidden.update(b for b in [*builds, *alias.get('retired_builds', [])] if b)
        if primary:
            names[primary] = alias['display_name']
    result = []
    for model in catalog['models']:
        meta = model.get('metadata') or {}
        if not model.get('active', True) or meta.get('hidden_from_picker') or meta.get('hide_standalone'):
            continue
        if model['id'] in hidden and model['id'] not in names:
            continue
        if 'rollback' in model.get('display_name', '').lower():
            continue
        row = dict(model)
        row['label'] = clean(names.get(model['id'], model.get('display_name') or model['id']))
        result.append(row)
    return result


def downloaded(model_id, cache):
    # Exact build IDs only: a similarly named HF repository may contain different
    # quantization/weights. Fast discovery, never hashing or loading large files.
    folder = cache / ('models--' + model_id.replace('/', '--')) / 'snapshots'
    if '..' in model_id.split('/'):
        return False
    candidates = sorted((p for p in folder.glob('*') if p.is_dir() and not p.name.startswith('.')),
                        key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return False
    snapshot = candidates[0]
    if not (snapshot / 'config.json').is_file():
        return False
    index = snapshot / 'model.safetensors.index.json'
    if index.exists():
        try:
            files = set(json.loads(index.read_text())['weight_map'].values())
            return bool(files) and all((snapshot / p).is_file() and (snapshot / p).stat().st_size > 0 for p in files)
        except (KeyError, ValueError, OSError):
            return False
    return any(p.stat().st_size > 0 for p in snapshot.glob('*.safetensors') if p.is_file())


def machine():
    physical = int(subprocess.check_output(['/usr/sbin/sysctl', '-n', 'hw.memsize'], text=True))
    chip = subprocess.check_output(['/usr/sbin/sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip()
    reserve = 4
    config = Path.home() / '.config/darkbloom/provider.toml'
    if config.exists():
        reserve = tomllib.loads(config.read_text()).get('provider', {}).get('memory_reserve_gb', reserve)
    return {'physicalBytes': physical,
            'reserveBytes': int(reserve * 2**30)}, clean(chip)


def estimates(machine_info, models):
    sources = ROOT / 'provider-swift/Sources/ProviderCore/Inference'
    with tempfile.TemporaryDirectory(prefix='darkbloom-preview-policy-') as temporary:
        executable = Path(temporary) / 'estimate'
        subprocess.run(['xcrun', 'swiftc', '-module-cache-path', temporary + '/modules',
                        str(sources / 'UnifiedMemoryCap.swift'), str(sources / 'ModelLoadAdmission.swift'),
                        str(Path(__file__).with_name('memory_estimate.swift')), '-o', str(executable)],
                       check=True, capture_output=True, text=True, timeout=120)
        payload = dict(machine_info, models=[{'id': m['id'], 'bytes': m['total_size_bytes']} for m in models])
        return json.loads(subprocess.check_output([str(executable)], input=json.dumps(payload), text=True))


def load(catalog_file=None):
    if catalog_file:
        catalog = json.loads(Path(catalog_file).read_text())
    else:
        with urllib.request.urlopen(URL, timeout=15) as response:
            data = response.read(4 * 1024 * 1024 + 1)
        if len(data) > 4 * 1024 * 1024:
            raise ValueError('Catalog response is too large')
        catalog = json.loads(data)
    models = public_models(catalog)
    info, chip = machine()
    policy = {e['id']: e for e in estimates(info, models)}
    cache = Path.home() / '.cache/huggingface/hub'
    for model in models:
        model['downloaded'] = downloaded(model['id'], cache)
        model['estimate'] = policy[model['id']]
        reasons = []
        if model.get('min_ram_gb', 0) > info['physicalBytes'] / 2**30:
            reasons.append(f"Catalog requires {model['min_ram_gb']} GB RAM")
        if not policy[model['id']]['fits']:
            reasons.append(f"Estimated {policy[model['id']]['requiredGiB']:.1f} GiB needed")
        requirements = model.get('required_provider_capabilities') or []
        if 'apple_m5' in requirements and 'M5' not in chip:
            reasons.append('Requires Apple M5')
        elif requirements:
            reasons.append('Runtime capability check needed: ' + ', '.join(map(clean, requirements)))
        model['reason'] = '; '.join(reasons)
    return models, info, chip
