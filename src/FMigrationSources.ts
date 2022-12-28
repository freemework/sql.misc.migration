import * as fs from "fs";
import * as path from "path";
import { promisify } from "util";
import { fileURLToPath } from "url";

import {
	FCancellationToken,
	FExecutionContext,
	FCancellationExecutionContext,
	FExceptionArgument,
	FExceptionInvalidOperation
} from "@freemework/common";

const existsAsync = promisify(fs.exists);
const mkdirAsync = promisify(fs.mkdir);
const readdirAsync = promisify(fs.readdir);
const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);

export class FMigrationSources {
	public readonly versionNames: ReadonlyArray<string>;
	private readonly _versions: Map<string, FMigrationSources.VersionBundle>;

	/**
	 * Load data into memory and represent it as FMigrationSources
	 * @param executionContext
	 * @param sourceUri Sources url. Support schemas `file:`, `http+tar+gz:` and `https+tar+gz:`
	 */
	public static load(executionContext: FExecutionContext, sourceUri: URL, opts?: {
		readonly versionFrom?: string;
		readonly versionTo?: string;
	}): Promise<FMigrationSources> {
		switch (sourceUri.protocol as UrlSchemas) {
			case UrlSchemas.FILE: {
				const sourceDirectory: string = fileURLToPath(sourceUri);
				return FMigrationSources.loadFromFilesystem(executionContext, sourceDirectory, opts);
			}
			case UrlSchemas.HTTP_TAR_GZ:
				throw new FExceptionInvalidOperation("Not implemented yet");
			case UrlSchemas.HTTPS_TAR_GZ:
				throw new FExceptionInvalidOperation("Not implemented yet");
			default:
				throw new NotSupportedUrlSchemaException(sourceUri);
		}
	}

	public static async loadFromFilesystem(
		executionContext: FExecutionContext, sourceDirectory: string, opts?: {
			readonly versionFrom?: string;
			readonly versionTo?: string;
		}
	): Promise<FMigrationSources> {
		if (!await existsAsync(sourceDirectory)) {
			throw new FMigrationSources.WrongMigrationDataException(`Migration directory '${sourceDirectory}' is not exist`);
		}

		const migrationBundles: Array<FMigrationSources.VersionBundle> = [];

		const listVersions: Array<string> = (await readdirAsync(sourceDirectory, { withFileTypes: true }))
			.filter(w => w.isDirectory())
			.map(directory => directory.name);

		const cancellationToken: FCancellationToken = FCancellationExecutionContext.of(executionContext).cancellationToken;

		if (listVersions.length > 0) {
			for (const version of listVersions) {
				if (opts !== undefined) {
					// Control version load range, skip out range versions
					if (opts.versionFrom !== undefined) {
						if (version < opts.versionFrom) {
							continue;
						}
					}
					if (opts.versionTo !== undefined) {
						if (version > opts.versionTo) {
							continue;
						}
					}
				}

				cancellationToken.throwIfCancellationRequested();
				const versionDirectory = path.join(sourceDirectory, version);

				const installDirectory = path.join(versionDirectory, FMigrationSources.Direction.INSTALL);
				const rollbackDirectory = path.join(versionDirectory, FMigrationSources.Direction.ROLLBACK);

				const installBundleItems: Array<FMigrationSources.Script> = [];
				const rollbackBundleItems: Array<FMigrationSources.Script> = [];

				if (await existsAsync(installDirectory)) {
					const migrationFiles = await readdirAsync(installDirectory);
					if (migrationFiles.length > 0) {
						for (const migrationFile of migrationFiles) {
							cancellationToken.throwIfCancellationRequested();
							const scriptFile = path.join(installDirectory, migrationFile);
							const scriptContent: string = await readFileAsync(scriptFile, "utf-8");
							const scriptKind: FMigrationSources.Script.Kind = resolveScriptKindByExtension(scriptFile);
							installBundleItems.push(
								new FMigrationSources.Script(migrationFile, scriptKind, scriptFile, scriptContent)
							);
						}
					}
				}
				if (await existsAsync(rollbackDirectory)) {
					const migrationFiles = await readdirAsync(rollbackDirectory);
					if (migrationFiles.length > 0) {
						for (const migrationFile of migrationFiles) {
							cancellationToken.throwIfCancellationRequested();
							const scriptFile = path.join(rollbackDirectory, migrationFile);
							const scriptContent: string = await readFileAsync(scriptFile, "utf-8");
							const scriptKind: FMigrationSources.Script.Kind = resolveScriptKindByExtension(scriptFile);
							rollbackBundleItems.push(
								new FMigrationSources.Script(migrationFile, scriptKind, scriptFile, scriptContent)
							);
						}
					}
				}

				migrationBundles.push(
					new FMigrationSources.VersionBundle(version, installBundleItems, rollbackBundleItems)
				);
			}
		}

		return new FMigrationSources(migrationBundles);
	}

	public getVersionBundle(versionName: string): FMigrationSources.VersionBundle {
		const item = this._versions.get(versionName);
		if (item === undefined) {
			throw new FExceptionArgument(`No version bundle with name: ${versionName}`, "versionName");
		}
		return item;
	}

	public map(
		callbackfn: (content: FMigrationSources.Script["content"], info: {
			readonly versionName: string;
			readonly direction: FMigrationSources.Direction;
			readonly itemName: string;
		}) => FMigrationSources.Script["content"]
	): FMigrationSources {
		const mappedBundles: Array<FMigrationSources.VersionBundle> = [];
		for (const versionName of this.versionNames) {
			const bundle: FMigrationSources.VersionBundle = this.getVersionBundle(versionName);
			const newBundle = bundle.map((item, opts) => {
				return callbackfn(item.content, Object.freeze({
					itemName: item.name,
					direction: opts.direction,
					versionName
				}));
			});
			mappedBundles.push(newBundle);
		}
		return new FMigrationSources(mappedBundles);
	}

	public async saveToFilesystem(
		executionContext: FExecutionContext, destinationDirectory: string
	): Promise<void> {
		if (!(await existsAsync(destinationDirectory))) {
			throw new FExceptionArgument(
				`Target directory '${destinationDirectory}' not exist. You must provide empty directory.`,
				"destinationDirectory"
			);
		}

		const cancellationToken: FCancellationToken = FCancellationExecutionContext.of(executionContext).cancellationToken;

		for (const versionName of this.versionNames) {
			cancellationToken.throwIfCancellationRequested();

			const versionDirectory: string = path.join(destinationDirectory, versionName);
			const installDirectory: string = path.join(versionDirectory, FMigrationSources.Direction.INSTALL);
			const rollbackDirectory: string = path.join(versionDirectory, FMigrationSources.Direction.ROLLBACK);

			await mkdirAsync(versionDirectory);
			cancellationToken.throwIfCancellationRequested();
			await mkdirAsync(installDirectory);
			cancellationToken.throwIfCancellationRequested();
			await mkdirAsync(rollbackDirectory);

			const versionBundle: FMigrationSources.VersionBundle = this.getVersionBundle(versionName);

			for (const installItemName of versionBundle.installScriptNames) {
				cancellationToken.throwIfCancellationRequested();
				const bundleFile = path.join(installDirectory, installItemName);
				const bundleItem: FMigrationSources.Script = versionBundle.getInstallScript(installItemName);
				await writeFileAsync(bundleFile, bundleItem.content, "utf-8");
			}

			for (const rollbackItemName of versionBundle.rollbackScriptNames) {
				cancellationToken.throwIfCancellationRequested();
				const bundleFile: string = path.join(rollbackDirectory, rollbackItemName);
				const bundleItem: FMigrationSources.Script = versionBundle.getRollbackScript(rollbackItemName);
				await writeFileAsync(bundleFile, bundleItem.content, "utf-8");
			}
		}
	}

	private constructor(bundles: Array<FMigrationSources.VersionBundle>) {
		this._versions = new Map(bundles.map(bundle => ([bundle.versionName, bundle])));
		this.versionNames = Object.freeze([...this._versions.keys()].sort());
	}
}

export namespace FMigrationSources {
	export class WrongMigrationDataException extends FExceptionInvalidOperation { }

	export class VersionBundle {
		public readonly versionName: string;
		public readonly installScriptNames: ReadonlyArray<string>;
		public readonly rollbackScriptNames: ReadonlyArray<string>;
		private readonly _installScripts: Map<string, Script>;
		private readonly _rollbackScripts: Map<string, Script>;

		public constructor(
			versionName: string,
			installItems: ReadonlyArray<Script>,
			rollbackItems: ReadonlyArray<Script>
		) {
			this.versionName = versionName;
			this._installScripts = new Map(installItems.map(installItem => ([installItem.name, installItem])));
			this._rollbackScripts = new Map(rollbackItems.map(rollbackItem => ([rollbackItem.name, rollbackItem])));
			this.installScriptNames = Object.freeze([...this._installScripts.keys()].sort());
			this.rollbackScriptNames = Object.freeze([...this._rollbackScripts.keys()].sort());
		}

		public getInstallScript(itemName: string): Script {
			const item = this._installScripts.get(itemName);
			if (item === undefined) {
				throw new FExceptionArgument(`No bundle item with name: ${itemName}`, "itemName");
			}
			return item;
		}

		public getRollbackScript(itemName: string): Script {
			const script = this._rollbackScripts.get(itemName);
			if (script === undefined) {
				throw new FExceptionArgument(`No bundle item with name: ${itemName}`, "itemName");
			}
			return script;
		}

		public map(
			callbackFn: (item: Script, opts: {
				readonly direction: FMigrationSources.Direction;
			}) => Script["content"]
		): VersionBundle {
			const installScripts: ReadonlyArray<Script> =
				VersionBundle._map(this.installScriptNames, this._installScripts, (item) => {
					return callbackFn(item, { direction: FMigrationSources.Direction.INSTALL });
				});
			const rollbackScripts: ReadonlyArray<Script> =
				VersionBundle._map(this.rollbackScriptNames, this._rollbackScripts, (item) => {
					return callbackFn(item, { direction: FMigrationSources.Direction.ROLLBACK });
				});

			return new VersionBundle(this.versionName, installScripts, rollbackScripts);
		}

		private static _map(
			itemNames: ReadonlyArray<string>,
			itemsMap: ReadonlyMap<string, Script>,
			callbackfn: (item: Script) => Script["content"]
		): ReadonlyArray<Script> {
			const mappedItems: Array<Script> = [];
			for (const itemName of itemNames) {
				const item: Script = itemsMap.get(itemName)!;
				const newContent: Script["content"] = callbackfn(item);
				mappedItems.push(new Script(item.name, item.kind, item.file, newContent));
			}
			return mappedItems;
		}
	}

	export class Script {
		public constructor(
			readonly name: string,
			readonly kind: Script.Kind,
			readonly file: string,
			readonly content: string
		) { }
	}
	export namespace Script {
		export enum Kind {
			SQL = "SQL",
			JAVASCRIPT = "JAVASCRIPT",
			UNKNOWN = "UNKNOWN"
		}
		export namespace Kind {
			export function guard(kind: string): kind is Kind {
				const friendlyValue: Kind = kind as Kind;
				switch (friendlyValue) {
					case Kind.SQL:
					case Kind.JAVASCRIPT:
					case Kind.UNKNOWN:
						return true;
					default:
						return guardFalse(friendlyValue);
				}
			}
			// tslint:disable-next-line: no-shadowed-variable
			export function parse(kind: string): Kind {
				const friendlyValue: Kind = kind as Kind;
				if (guard(friendlyValue)) { return friendlyValue; }
				throw new UnreachableNotSupportedScriptKindException(friendlyValue);
			}
			export class UnreachableNotSupportedScriptKindException extends FExceptionArgument {
				public constructor(kind: never) {
					super(`Not supported script kind '${JSON.stringify(kind)}'`, "fiatCurrency");
				}
			}
			function guardFalse(_never: never): false { return false; }
		}
	}

	export const enum Direction {
		INSTALL = "install",
		ROLLBACK = "rollback"
	}
}


const enum UrlSchemas {
	FILE = "file:",
	HTTP_TAR_GZ = "http+tar+gz:",
	HTTPS_TAR_GZ = "https+tar+gz:"
}

class NotSupportedUrlSchemaException extends FExceptionInvalidOperation {
	public constructor(uri: URL) {
		super(`Not supported schema: ${uri}`);
	}
}


const sqlFilesExtensions = Object.freeze([".sql"]);
const jsFilesExtensions = Object.freeze([".js"]);
function resolveScriptKindByExtension(fileName: string): FMigrationSources.Script.Kind {
	const ext = path.extname(fileName);
	if (sqlFilesExtensions.includes(ext)) {
		return FMigrationSources.Script.Kind.SQL;
	}
	if (jsFilesExtensions.includes(ext)) {
		return FMigrationSources.Script.Kind.JAVASCRIPT;

	}
	return FMigrationSources.Script.Kind.UNKNOWN;
}
