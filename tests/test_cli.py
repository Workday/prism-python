from click.testing import CliRunner
from prism.cli import main

def test_cli():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0