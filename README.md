# Freemework

[Freemework](https://docs.freemework.org) is a general purposes framework with goal to provide cross language API. Learn API once - develop for any programming language.

## Freemework SQL Migration Library

This is workspace branch of **Freemework SQL Migration Library** multi project repository based on [orphan](https://git-scm.com/docs/git-checkout#Documentation/git-checkout.txt---orphanltnew-branchgt) branches.

Branches (sub-projects):

* `docs` - Sources of library [documentation](https://docs.freemework.org/sql.misc.migration).
* `src-csharp` - C# Sources
* `src-dart` - Dart Sources
* `src-python` - Python Sources
* `src-typescript` - TypeScript Sources

## Get Started

```shell
git clone git@github.com:freemework/sql.misc.migration.git freemework.sql.misc.migration
cd freemework.sql.misc.migration
for BRANCH in docs src-csharp src-dart src-python src-typescript; do git worktree add "${BRANCH}" "${BRANCH}"; done
code "Freemework-Sql.Misc.Migration.code-workspace"
```
