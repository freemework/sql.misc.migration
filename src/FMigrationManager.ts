import { EOL } from "os";
import * as path from "path";
import * as vm from "vm";

import {
	FExecutionContext,
	FLogger,
	FSqlProvider,
	FSqlProviderFactory,
	FException
} from "@freemework/common";

import { FMigrationSources } from "./FMigrationSources";

export abstract class FMigrationManager {
	private readonly _sqlProviderFactory: FSqlProviderFactory;
	private readonly _migrationSources: FMigrationSources;
	private readonly _log: FLogger;
	private readonly _versionTableName: string;


	public constructor(opts: FMigrationManager.Opts) {
		this._migrationSources = opts.migrationSources;
		this._sqlProviderFactory = opts.sqlProviderFactory;
		this._log = opts.log;
		this._versionTableName = opts.versionTableName !== undefined ? opts.versionTableName : "__migration";
	}

	/**
	 * Install versions (increment version)
	 * @param executionContext
	 * @param targetVersion Optional target version. Will use latest version if omited.
	 */
	public async install(executionContext: FExecutionContext, targetVersion?: string): Promise<void> {
		const currentVersion: string | null = await this.getCurrentVersion(executionContext);
		const availableVersions: Array<string> = [...this._migrationSources.versionNames].sort(); // from old version to new version
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

		await this.sqlProviderFactory.usingProvider(executionContext, async (sqlProvider: FSqlProvider) => {
			if (!(await this._isVersionTableExist(executionContext, sqlProvider))) {
				await this._createVersionTable(executionContext, sqlProvider);
			}
		});

		for (const versionName of scheduleVersions) {
			await this.sqlProviderFactory.usingProviderWithTransaction(executionContext, async (sqlProvider: FSqlProvider) => {
				const migrationLogger = new FMigrationManager.MigrationLogger(this._log.getLogger(versionName));

				const versionBundle: FMigrationSources.VersionBundle = this._migrationSources.getVersionBundle(versionName);
				const installScriptNames: Array<string> = [...versionBundle.installScriptNames].sort();
				for (const scriptName of installScriptNames) {
					const script: FMigrationSources.Script = versionBundle.getInstallScript(scriptName);
					switch (script.kind) {
						case FMigrationSources.Script.Kind.SQL: {
							migrationLogger.info(`Execute SQL script: ${script.name}`);
							migrationLogger.trace(EOL + script.content);
							await this._executeMigrationSql(executionContext, sqlProvider, migrationLogger, script.content);
							break;
						}
						case FMigrationSources.Script.Kind.JAVASCRIPT: {
							migrationLogger.info(`Execute JS script: ${script.name}`);
							migrationLogger.trace(EOL + script.content);
							await this._executeMigrationJavaScript(
								executionContext, sqlProvider, migrationLogger,
								{
									content: script.content,
									file: script.file
								}
							);
							break;
						}
						default:
							migrationLogger.warn(`Skip script '${versionName}:${script.name}' due unknown kind of script`);
					}
				}

				const logText: string = migrationLogger.flush();
				await this._insertVersionLog(executionContext, sqlProvider, versionName, logText);
			});
		}
	}

	/**
	 * Rollback versions (increment version)
	 * @param cancellationToken A cancellation token that can be used to cancel the action.
	 * @param targetVersion Optional target version. Will use latest version if omited.
	 */
	public async rollback(executionContext: FExecutionContext, targetVersion?: string): Promise<void> {
		const currentVersion: string | null = await this.getCurrentVersion(executionContext);
		const availableVersions: Array<string> = [...this._migrationSources.versionNames].sort().reverse(); // from new version to old version
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
			await this.sqlProviderFactory.usingProviderWithTransaction(executionContext, async (sqlProvider: FSqlProvider) => {
				if (! await this._isVersionLogExist(executionContext, sqlProvider, versionName)) {
					this._log.warn(`Skip rollback for version '${versionName}' due this does not present inside database.`);
					return;
				}

				const versionBundle: FMigrationSources.VersionBundle = this._migrationSources.getVersionBundle(versionName);
				const rollbackScriptNames: Array<string> = [...versionBundle.rollbackScriptNames].sort();
				for (const scriptName of rollbackScriptNames) {
					const migrationLogger = this._log.getLogger(versionName);

					const script: FMigrationSources.Script = versionBundle.getRollbackScript(scriptName);
					switch (script.kind) {
						case FMigrationSources.Script.Kind.SQL: {
							migrationLogger.info(`Execute SQL script: ${script.name}`);
							migrationLogger.trace(EOL + script.content);
							await this._executeMigrationSql(executionContext, sqlProvider, migrationLogger, script.content);
							break;
						}
						case FMigrationSources.Script.Kind.JAVASCRIPT: {
							migrationLogger.info(`Execute JS script: ${script.name}`);
							migrationLogger.trace(EOL + script.content);
							await this._executeMigrationJavaScript(
								executionContext, sqlProvider, migrationLogger,
								{
									content: script.content,
									file: script.file
								}
							);
							break;
						}
						default:
							migrationLogger.warn(`Skip script '${versionName}:${script.name}' due unknown kind of script`);
					}
				}

				await this._removeVersionLog(executionContext, sqlProvider, versionName);
			});
		}
	}

	/**
	 * Gets current version of the database or `null` if version table is not presented.
	 * @param cancellationToken Allows to request cancel of the operation.
	 */
	public abstract getCurrentVersion(executionContext: FExecutionContext): Promise<string | null>;

	protected get sqlProviderFactory(): FSqlProviderFactory { return this._sqlProviderFactory; }

	protected get log(): FLogger { return this._log; }

	protected get versionTableName(): string { return this._versionTableName; }

	protected abstract _createVersionTable(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider
	): Promise<void>;

	protected async _executeMigrationJavaScript(
		executionContext: FExecutionContext,
		sqlProvider: FSqlProvider,
		migrationLogger: FLogger,
		migrationJavaScript: {
			readonly content: string;
			readonly file: string;
		}
	): Promise<void> {
		await new Promise<void>((resolve, reject) => {
			const sandbox = {
				__private: { executionContext, log: migrationLogger, resolve, reject, sqlProvider },
				__dirname: path.dirname(migrationJavaScript.file),
				__filename: migrationJavaScript.file
			};
			const script = new vm.Script(`${migrationJavaScript.content}
migration(__private.cancellationToken, __private.sqlProvider, __private.log).then(__private.resolve).catch(__private.reject);`,
				{
					filename: migrationJavaScript.file
				}
			);
			script.runInNewContext(sandbox, { displayErrors: false });
		});
	}

	protected async _executeMigrationSql(
		executionContext: FExecutionContext,
		sqlProvider: FSqlProvider,
		migrationLogger: FLogger,
		sqlText: string
	): Promise<void> {
		migrationLogger.trace(EOL + sqlText);
		await sqlProvider.statement(sqlText).execute(executionContext);
	}

	protected abstract _insertVersionLog(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider, version: string, logText: string
	): Promise<void>;

	protected abstract _isVersionLogExist(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider, version: string
	): Promise<boolean>;

	protected abstract _isVersionTableExist(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider
	): Promise<boolean>;

	protected abstract _removeVersionLog(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider, version: string
	): Promise<void>;

	protected abstract _verifyVersionTableStructure(
		executionContext: FExecutionContext, sqlProvider: FSqlProvider
	): Promise<void>;
}

export namespace FMigrationManager {
	export interface Opts {
		readonly migrationSources: FMigrationSources;

		readonly sqlProviderFactory: FSqlProviderFactory;

		readonly log: FLogger;

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

		public get context(): FLogger.Context {
			return this._wrap.context;
		}

		public flush(): string {
			// Join and empty _lines
			return this._lines.splice(0).join(EOL);
		}

		public trace(message: string, ex?: FException): void {
			this._wrap.trace(message, ex);
			this._lines.push("[TRACE] " + message);
		}
		public debug(message: string, ex?: FException): void {
			this._wrap.debug(message, ex);
			this._lines.push("[DEBUG] " + message);
		}
		public info(message: string): void {
			this._wrap.info(message);
			this._lines.push("[INFO] " + message);
		}
		public warn(message: string): void {
			this._wrap.warn(message);
			this._lines.push("[WARN] " + message);
		}
		public error(message: string): void {
			this._wrap.error(message);
			this._lines.push("[ERROR] " + message);
		}
		public fatal(message: string): void {
			this._wrap.fatal(message);
			this._lines.push("[FATAL]" + message);
		}

		public getLogger(name: string): FLogger;
		public getLogger(name: string, context: FLogger.Context): FLogger;
		public getLogger(context: FLogger.Context): FLogger;
		public getLogger(name: unknown, context?: unknown): FLogger {
			return this;
		}
	}
}
