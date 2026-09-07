import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from install import Terminal
from live_catalog import downloaded, public_models
from live_models import choose, groups, screen


def model(name, local=False, reason=''):
    return dict(id=name, label=name, total_size_bytes=10**9, downloaded=local,
                reason=reason, estimate={'availableGiB': 20})


class LiveModelsTests(unittest.TestCase):
    def test_sections_and_opt_in(self):
        models = [model('Fits'), model('Large', reason='Estimated 30 GiB needed')]
        info = dict(physicalBytes=48*2**30)
        output = '\n'.join(v for v, _ in screen(models, info, 'M4', set(), False, 0))
        self.assertIn('based on total RAM', output)
        self.assertNotIn('available at this check', output)
        self.assertNotIn('Downloaded', output)
        self.assertNotIn('Large', output)
        self.assertIn('Available to download', output)
        self.assertIn('Show 1 additional', output)
        # Hidden rows cannot be toggled by number until explicitly expanded.
        with patch('sys.stdin', io.StringIO('2\nh\n2\n\n')), patch('sys.stdout', io.StringIO()):
            selected = choose(Terminal(True), models, info, 'M4')
        self.assertEqual([m['id'] for m in selected], ['Large'])

    def test_downloaded_stays_visible_even_if_large(self):
        models = [model('Local', local=True, reason='Too large'), model('Small')]
        self.assertEqual(groups(models)[0][1], [0])
        self.assertEqual(groups(models)[2][1], [])

    def test_alias_hides_old_build(self):
        catalog = dict(models=[{'id': 'old'}, {'id': 'new'}, {'id': 'other'}],
                       aliases=[dict(desired_build='new', previous_build='old', display_name='Public name')])
        rows = public_models(catalog)
        self.assertEqual([m['id'] for m in rows], ['new', 'other'])
        self.assertEqual(rows[0]['label'], 'Public name')

    def test_incomplete_shards_not_downloaded(self):
        with tempfile.TemporaryDirectory() as root:
            cache = Path(root)
            snap = cache / 'models--example/snapshots/local'
            snap.mkdir(parents=True)
            (snap / 'config.json').write_text('{}')
            (snap / 'one.safetensors').write_bytes(b'fixture')
            (snap / 'model.safetensors.index.json').write_text('{"weight_map":{"a":"one.safetensors","b":"two.safetensors"}}')
            self.assertFalse(downloaded('example', cache))
            (snap / 'two.safetensors').write_bytes(b'fixture')
            self.assertTrue(downloaded('example', cache))


if __name__ == '__main__':
    unittest.main()
