# Development Guideline

## NOTE

* Do **NOT** directly work on your master branch. Your master branch should **ONLY** be
updated by merging with latest codebase.

## Fork Repository

* Fork [USTORE Github repository](https://github.com/nusdbsystem/USTORE) to your own Github account.

* Clone your forked repository to your dev machine:
```
  git clone https://github.com/YOUR_GITHUB_ACCOUNT/USTORE.git
```

## Code for a Task

* Sync your local master branch with latest codebase:
```
  # change to master branch
  git checkout master
  # fetch latest master codebase
  git fetch nusdbsystem master
  # apply changes to local master branch
  git merge --no-commit FETCH_HEAD
```

* Create a new branch from master (e.g., feature-foo or fixbug-foo) to work on:
```
  # change to master branch
  git checkout master
  git branch feature-foo
  # change to feature-foo branch
  git checkout feature-foo
```

* Code and test on that branch.

## Prepare for Submission

* Ensure your code pass all unit tests.

* Check coding style of changed files:
```
  # at USTORE root dir
  tool/cpplint.py FILE_NAME
```

* Rebase your local commits into one or several well-maintained commits
```
  # merge multiple commits into less ones
  # X equal to the number of local commits want to submit
  git rebase -i HEAD~X
```

* Rebase your branch to latest codebase:
```
  # fetch latest master codebase
  git fetch nusdbsystem master
  # update local master to latest codebase
  git checkout master
  git merge --no-commit FETCH_HEAD
  # rebase to latest codebase and resolve conflict
  git checkout feature-foo
  git rebase master
```

## Submit Pull Request

* Push working branch to your own github repository
```
  # push changes to remote repository
  git push YOUR_REPO feature-foo
```

* Browse on Github to create a pull request from branch `feature-foo`

## Asked for Revising Pull Request

* After revision is done, go through all steps in **Prepare for Submission** phase.

* You might need to [rebase your pull request](https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request).
If you rebased your pull request, use `force` tag to overwrite previous branch.
```
  # push changes to remote repository and overwrite previous changes
  git push -f YOUR_REPO feature-foo
```
