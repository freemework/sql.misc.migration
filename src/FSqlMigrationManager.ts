import { EOL } from "os";
import * as path from "path";
import * as vm from "vm";

import {
	FExecutionContext,
	FLogger,
	FSqlConnection,
	FSqlConnectionFactory,
	FException,
	FLoggerLabels,
	FLoggerMessageFactory,
} from "@freemework/common";

import { FSqlMigrationSources } from "./FSqlMigrationSources";

export abstract class FSqlMigrationManager {
	private readonly _sqlConnectionFactory: FSqlConnectionFactory;
	private readonly _versionTableName: string;


	public constructor(opts: FSqlMigrationManager.Opts) {
		this._sqlConnectionFactory = opts.sqlConnectionFactory;
		this._versionTableName = opts.versionTableName !== undefined ? opts.versionTableName : "__migration";
	}

	/**
	 * Install versions (increment version)
	 * @param executionContext
	 * @param targetVersion Optional target version. Will use latest version if omitted.
	 */
	public async install(executionContext: FExecutionContext, migrationSources: FSqlMigrationSources, targetVersion?: string): Promise<void> {
		const logger: FLogger = FLogger.create("install");
		const currentVersion: string | null = await this.getCurrentVersion(executionContext);
		const availableVersions: Array<string> = [...migrationSources.versionNames].sort(); // from old version to new version
		let scheduleVersions: Array<string> = availableVersions;

		if (currentVersion !== null) {
			scheduleVersions = scheduleVersions.reduce<Array<string>>(
				(p, c) => { if (c > currentVersion) { p.push(c); } return p; },
				[]
			);
		}

		if (targetVersion !== undefined) {
			scheduleVersions = scheduleVersions.reduceRight<Array<string>>(function (p, c) {
				if (c <= targetVersion) { p.unshift(c); } return p;
			}, []);
		}

		await this.sqlConnectionFactory.usingProvider(executionContext, async (sqlConnection: FSqlConnection) => {
			if (!(await this._isVersionTableExist(executionContext, sqlConnection))) {
				await this._createVersionTable(executionContext, sqlConnection);
			}
		});

		for (const versionName of scheduleVersions) {
			await this.sqlConnectionFactory.usingProviderWithTransaction(executionContext, async (sqlConnection: FSqlConnection) => {
				const migrationLogger = new FSqlMigrationManager.MigrationLogger(FLogger.create(`${logger.name}.${versionName}`));

				const versionBundle: FSqlMigrationSources.VersionBundle = migrationSources.getVersionBundle(versionName);
				const installScriptNames: Array<string> = [...versionBundle.installScriptNames].sort();
				for (const scriptName of installScriptNames) {
					const script: FSqlMigrationSources.Script = versionBundle.getInstallScript(scriptName);
					switch (script.kind) {
						case FSqlMigrationSources.Script.Kind.SQL: {
							migrationLogger.info(executionContext, `Execute SQL script: ${script.name}`);
							migrationLogger.trace(executionContext, EOL + script.content);
							await this._executeMigrationSql(executionContext, sqlConnection, migrationLogger, script.content);
							break;
						}
						case FSqlMigrationSources.Script.Kind.JAVASCRIPT: {
							migrationLogger.info(executionContext, `Execute JS script: ${script.name}`);
							migrationLogger.trace(executionContext, EOL + script.content);
							await this._executeMigrationJavaScript(
								executionContext, sqlConnection, migrationLogger,
								{
									content: script.content,
									file: script.file
								}
							);
							break;
						}
						default:
							migrationLogger.warn(executionContext, `Skip script '${versionName}:${script.name}' due unknown kind of script`);
					}
				}

				const logText: string = migrationLogger.flush();
				await this._insertVersionLog(executionContext, sqlConnection, versionName, logText);

				const rollbackScripts: Array<FSqlMigrationSources.Script> = versionBundle.rollbackScriptNames.map(scriptName => versionBundle.getRollbackScript(scriptName));
				await this._insertRollbackScripts(executionContext, sqlConnection, versionName, rollbackScripts);
			});
		}
	}

	/**
	 * Rollback versions (increment version)
	 * @param cancellationToken A cancellation token that can be used to cancel the action.
	 * @param targetVersion Optional target version. Will use first version if omitted.
	 */
	public async rollback(executionContext: FExecutionContext, targetVersion?: string): Promise<void> {
		const logger: FLogger = FLogger.create("rollback");
		const currentVersion: string | null = await this.getCurrentVersion(executionContext);

		const versionNames: Array<string> = await this.sqlConnectionFactory.usingProvider(executionContext,
			(sqlConnection: FSqlConnection) => this._listVersions(executionContext, sqlConnection)
		);

		const availableVersions: Array<string> = [...versionNames].sort().reverse(); // from new version to old version
		let scheduleVersionNames: Array<string> = availableVersions;

		if (currentVersion !== null) {
			scheduleVersionNames = scheduleVersionNames.reduce<Array<string>>(
				(p, c) => { if (c <= currentVersion) { p.push(c); } return p; },
				[]
			);
		}

		if (targetVersion !== undefined) {
			scheduleVersionNames = scheduleVersionNames.reduceRight<Array<string>>(
				(p, c) => { if (c > targetVersion) { p.unshift(c); } return p; },
				[]
			);
		}

		for (const versionName of scheduleVersionNames) {
			await this.sqlConnectionFactory.usingProviderWithTransaction(executionContext, async (sqlConnection: FSqlConnection) => {
				if (! await this._isVersionLogExist(executionContext, sqlConnection, versionName)) {
					logger.warn(executionContext, `Skip rollback for version '${versionName}' due this does not present inside database.`);
					return;
				}

				const scripts: Array<FSqlMigrationSources.Script> = await this._getRollbackScripts(executionContext, sqlConnection, versionName);
				//const versionBundle: FSqlMigrationSources.VersionBundle = this._migrationSources.getVersionBundle(versionName);
				const rollbackScriptNames: Array<string> = [...scripts.map(s => s.name)].sort();
				const scriptsMap: Map<string, FSqlMigrationSources.Script> = scripts.reduce((acc, curr) => { acc.set(curr.name, curr); return acc }, new Map<string, FSqlMigrationSources.Script>());
				for (const scriptName of rollbackScriptNames) {
					const migrationLogger: FLogger = FLogger.create(`${logger.name}.${versionName}`);

					const script: FSqlMigrationSources.Script = scriptsMap.get(scriptName)!;
					switch (script.kind) {
						case FSqlMigrationSources.Script.Kind.SQL: {
							migrationLogger.info(executionContext, `Execute SQL script: ${script.name}`);
							migrationLogger.trace(executionContext, EOL + script.content);
							await this._executeMigrationSql(executionContext, sqlConnection, migrationLogger, script.content);
							break;
						}
						case FSqlMigrationSources.Script.Kind.JAVASCRIPT: {
							migrationLogger.info(executionContext, `Execute JS script: ${script.name}`);
							migrationLogger.trace(executionContext, EOL + script.content);
							await this._executeMigrationJavaScript(
								executionContext, sqlConnection, migrationLogger,
								{
									content: script.content,
									file: script.file
								}
							);
							break;
						}
						default:
							migrationLogger.warn(executionContext, `Skip script '${versionName}:${script.name}' due unknown kind of script`);
					}
				}

				await this._removeVersionLog(executionContext, sqlConnection, versionName);
			});
		}
	}

	/**
	 * Gets current version of the database or `null` if version table is not presented.
	 * @param cancellationToken Allows to request cancel of the operation.
	 */
	public abstract getCurrentVersion(executionContext: FExecutionContext): Promise<string | null>;

	protected get sqlConnectionFactory(): FSqlConnectionFactory { return this._sqlConnectionFactory; }

	protected get versionTableName(): string { return this._versionTableName; }

	protected abstract _createVersionTable(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection
	): Promise<void>;

	protected async _executeMigrationJavaScript(
		executionContext: FExecutionContext,
		sqlConnection: FSqlConnection,
		migrationLogger: FLogger,
		migrationJavaScript: {
			readonly content: string;
			readonly file: string;
		}
	): Promise<void> {
		await new Promise<void>((resolve, reject) => {
			const sandbox = {
				__private: { executionContext, log: migrationLogger, resolve, reject, sqlConnection: sqlConnection },
				__dirname: path.dirname(migrationJavaScript.file),
				__filename: migrationJavaScript.file
			};
			const script = new vm.Script(`${migrationJavaScript.content}
Promise.resolve().then(() => migration(__private.executionContext, __private.sqlConnection, __private.log)).then(__private.resolve).catch(__private.reject);`,
				{
					filename: migrationJavaScript.file
				}
			);
			script.runInNewContext(sandbox, { displayErrors: false });
		});
	}

	protected async _executeMigrationSql(
		executionContext: FExecutionContext,
		sqlConnection: FSqlConnection,
		migrationLogger: FLogger,
		sqlText: string
	): Promise<void> {
		migrationLogger.trace(executionContext, EOL + sqlText);
		await sqlConnection.statement(sqlText).execute(executionContext);
	}

	protected abstract _getRollbackScripts(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection, version: string
	): Promise<Array<FSqlMigrationSources.Script>>;


	protected abstract _insertRollbackScripts(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection, version: string, scripts: ReadonlyArray<FSqlMigrationSources.Script>
	): Promise<void>;

	protected abstract _insertVersionLog(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection, version: string, logText: string
	): Promise<void>;

	protected abstract _isVersionLogExist(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection, version: string
	): Promise<boolean>;

	protected abstract _isVersionTableExist(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection
	): Promise<boolean>;

	protected abstract _listVersions(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection
	): Promise<Array<string>>;

	protected abstract _removeVersionLog(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection, version: string
	): Promise<void>;

	protected abstract _verifyVersionTableStructure(
		executionContext: FExecutionContext, sqlConnection: FSqlConnection
	): Promise<void>;
}

export namespace FSqlMigrationManager {
	export interface Opts {
		readonly migrationSources: FSqlMigrationSources;

		readonly sqlConnectionFactory: FSqlConnectionFactory;

		/**
		 * Name of version table. Default `__migration`.
		 */
		readonly versionTableName?: string;
	}

	export class MigrationException extends FException { }
	export class WrongMigrationDataException extends MigrationException { }

	export class MigrationLogger implements FLogger {
		public readonly isTraceEnabled: boolean;
		public readonly isDebugEnabled: boolean;
		public readonly isInfoEnabled: boolean;
		public readonly isWarnEnabled: boolean;
		public readonly isErrorEnabled: boolean;
		public readonly isFatalEnabled: boolean;

		private readonly _wrap: FLogger;
		private readonly _lines: Array<string>;

		public constructor(wrap: FLogger) {
			this.isTraceEnabled = true;
			this.isDebugEnabled = true;
			this.isInfoEnabled = true;
			this.isWarnEnabled = true;
			this.isErrorEnabled = true;
			this.isFatalEnabled = true;

			this._lines = [];
			this._wrap = wrap;
		}

		public get name(): string | null {
			return this._wrap.name;
		}

		public flush(): string {
			// Join and empty _lines
			return this._lines.splice(0).join(EOL);
		}

		public trace(executionContext: FExecutionContext, message: string | FLoggerMessageFactory, ex?: FException): void {
			this._wrap.trace(executionContext, message as any, ex);
			this._lines.push("[TRACE] " + message);
		}
		public debug(executionContext: FExecutionContext, message: string | FLoggerMessageFactory, ex?: FException): void {
			this._wrap.debug(executionContext, message as any, ex);
			this._lines.push("[DEBUG] " + message);
		}
		public info(executionContext: FExecutionContext, message: string | FLoggerMessageFactory): void {
			this._wrap.info(executionContext, message as any);
			this._lines.push("[INFO] " + message);
		}
		public warn(executionContext: FExecutionContext, message: string | FLoggerMessageFactory): void {
			this._wrap.warn(executionContext, message as any);
			this._lines.push("[WARN] " + message);
		}
		public error(executionContext: FExecutionContext, message: string | FLoggerMessageFactory): void {
			this._wrap.error(executionContext, message as any);
			this._lines.push("[ERROR] " + message);
		}
		public fatal(executionContext: FExecutionContext, message: string | FLoggerMessageFactory): void {
			this._wrap.fatal(executionContext, message as any);
			this._lines.push("[FATAL]" + message);
		}

		// public getLogger(name: string): FLoggerLegacy;
		// public getLogger(name: string, context: FLoggerLegacy.Context): FLoggerLegacy;
		// public getLogger(context: FLoggerLegacy.Context): FLoggerLegacy;
		// public getLogger(name: unknown, context?: unknown): FLoggerLegacy {
		// 	return this;
		// }
	}
}
