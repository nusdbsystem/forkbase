# Development Guideline

## NOTE

* Do **NOT** directly work on your master branch. Your master branch should **ONLY** be
updated by merging with the latest codebase.

## Fork Repository

* Fork [ForkBase Github repository](https://github.com/forkbase/forkbase) to your own Github account.

* Clone your forked repository to your dev machine:
```
  git clone https://github.com/YOUR_GITHUB_ACCOUNT/forkbase.git
```

## Code for a Task

* Sync your local master branch with the latest codebase:
```
  # change to the master branch
  git checkout master
  # fetch the latest master codebase
  git fetch forkbase master
  # apply changes to the local master branch
  git merge --no-commit FETCH_HEAD
```

* Create a new branch from master (e.g., feature-foo or fixbug-foo) to work on:
```
  # change to the master branch
  git checkout master
  git branch feature-foo
  # change to feature-foo branch
  git checkout feature-foo
```

* Code and test on that branch.

## Prepare for Submission

* Ensure your code passes all unit tests.

* Check the coding style of changed files:
```
  # at forkbase root dir
  tool/cpplint.py FILE_NAME
```

* Rebase your local commits into one or several well-maintained commits
```
  # merge multiple commits into fewer ones
  # X equal to the number of local commits to submit
  git rebase -i HEAD~X
```

* Rebase your branch to the latest codebase:
```
  # fetch the latest master codebase
  git fetch forkbase master
  # update local master to latest codebase
  git checkout master
  git merge --no-commit FETCH_HEAD
  # rebase to the latest codebase and resolve conflict
  git checkout feature-foo
  git rebase master
```

## Submit Pull Request

* Push the working branch to your own GitHub repository
```
  # push changes to the remote repository
  git push YOUR_REPO feature-foo
```

* Browse on Github to create a pull request from branch `feature-foo`

## Asked for Revising Pull Request

* After revision is done, go through all steps in **Prepare for Submission** phase.

* You might need to [rebase your pull request](https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request).
If you re-based your pull request, use `force` tag to overwrite the previous branch.
```
  # push changes to the remote repository and overwrite previous changes
  git push -f YOUR_REPO feature-foo
```
