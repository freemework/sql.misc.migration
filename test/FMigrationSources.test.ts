import * as Mustache from "mustache";

import { assert } from "chai";
import * as fs from "fs";
import * as path from "path";
import { pathToFileURL } from "url";

import { FMigrationSources } from "../src";
import { FCancellationExecutionContext, FExecutionContext } from "@freemework/common";

interface A {
}
namespace A {
	export const Dummy = null;
	export function factory(): A {
		return new B();
	}
}
class B implements A {
	public constructor() {}
}

const a: A = A.factory();


describe("FMigrationSources tests", function () {

	it("load() from disk", async function () {
		const resourcesDir: string = path.join(__dirname, "../test.files/FMigrationSources");

		const migrationSources: FMigrationSources = await FMigrationSources
			.load(
				FExecutionContext.Default,
				pathToFileURL(resourcesDir)
			);

		assert.equal(migrationSources.versionNames.length, 3);
		assert.equal(migrationSources.versionNames[0], "v0001");
		assert.equal(migrationSources.versionNames[1], "v0002");
		assert.equal(migrationSources.versionNames[2], "vXXXX");

		const transformedSources = migrationSources.map(
			(content, opts) => {
				return Mustache.render(content, {
					...opts,
					name: opts.itemName,
					musketeers: ["Athos", "Aramis", "Porthos", "D'Artagnan"]
				});
			}
		);

		assert.equal(transformedSources.versionNames.length, 3);
		assert.equal(transformedSources.versionNames[0], "v0001");
		assert.equal(transformedSources.versionNames[1], "v0002");
		assert.equal(transformedSources.versionNames[2], "vXXXX");

		assert.equal(
			transformedSources.getVersionBundle("vXXXX").getRollbackScript("2-drop-something.js").content,
			"// 2-drop-something.js rollback \n"
		);

		//const destDir = resourcesDir + "." + Date.now();
		//fs.mkdirSync(destDir);
		//await transformedSources.saveToFilesystem(DUMMY_CANCELLATION_TOKEN, destDir);
	});

});
