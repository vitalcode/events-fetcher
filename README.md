# events-fetcher


# GIT info

show differences between working tree and index, changes you haven't staged to commit
```
git diff [filename]
```

show differences between index and current commit, changes you're about to commit
```
git diff --cached [filename]
```

show differences between working tree and current commit
```
git diff HEAD [filename]
```

###git add
Don’t actually add the file(s), just show if they exist and/or will be ignored.
```
git add . --dry-run
```