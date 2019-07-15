<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\Regex;
use namespace HH\Lib\C;
use type Facebook\CLILib\CLIWithArguments;
use namespace Facebook\CLILib\CLIOptions;
use type Facebook\HackCodegen\{HackCodegenFactory, HackCodegenConfig, HackBuilderValues, HackBuilderKeys};

final class BuildSchemaCLI extends CLIWithArguments {
	private string $constName = 'DB_SCHEMA';

	<<__Override>>
	protected function getSupportedOptions(): vec<CLIOptions\CLIOption> {
		return vec[CLIOptions\with_required_string(
			$name ==> {
				$this->constName = $name;
			},
			'The name of the constant to generate. Defaults to DB_SCHEMA',
			'--name',
		)];
	}

	<<__Override>>
	public async function mainAsync(): Awaitable<int> {
		$terminal = $this->getTerminal();
		$stderr = $this->getStderr();

		if (C\is_empty($this->getArguments())) {
			$program = $this->getArgv()[0];
			await $terminal->getStdout()->writeAsync(<<<EOT

Usage: {$program} [--name DB_SCHEMA] [files...] > schema.hack

Files should be named [database_name].sql


EOT
			);

			return 0;
		}

		$generator = new SchemaGenerator();
		$generated = dict[];

		foreach ($this->getArguments() as $file) {
			$match = Regex\first_match($file, re"/^(.*?)\.sql$/");

			if ($match === null) {
				/* HHAST_IGNORE_ERROR[DontAwaitInALoop] */
				await $terminal->getStderr()
					->writeAsync("Expected file name matching [database_name].sql, {$file} does not match");
				return 1;
			}

			$db = $match[1];

			$contents = \file_get_contents($file);

			if ($contents === false) {
				/* HHAST_IGNORE_ERROR[DontAwaitInALoop] */
				await $terminal->getStderr()->writeAsync("File could not be loaded: {$contents}");
				return 1;
			}

			$schema = $generator->generateFromString($contents);
			$generated[$db] = $schema;
		}

		$cg = new HackCodegenFactory(new HackCodegenConfig());

		$generated = $cg->codegenConstant($this->constName)
			->setType("dict<string, dict<string, table_schema>>")
			->setValue($generated, HackBuilderValues::dict(HackBuilderKeys::export(), HackBuilderValues::dict(
				HackBuilderKeys::export(),
				// special exporters are required to make shapes and enum values, ::export would turn them into arrays and strings
				HackBuilderValues::shapeWithPerKeyRendering(
					shape(
						'name' => HackBuilderValues::export(),
						'indexes' => HackBuilderValues::vec(
							HackBuilderValues::shapeWithUniformRendering(HackBuilderValues::export()),
						),
						'fields' => HackBuilderValues::vec(HackBuilderValues::shapeWithPerKeyRendering(
							shape(
								'name' => HackBuilderValues::export(),
								'type' => HackBuilderValues::lambda(($cfg, $str) ==> 'DataType::'.$str),
								'length' => HackBuilderValues::export(),
								'null' => HackBuilderValues::export(),
								'hack_type' => HackBuilderValues::export(),
								'default' => HackBuilderValues::export(),
							),
						)),
					),
				),
			)))
			->render();

		$generated = <<<EOT
<?hh // strict

use type Slack\\SQLFake\\{table_schema, DataType};


EOT
			.
			$generated;

		await $terminal->getStdout()->writeAsync($generated);
		return 0;
	}
}
