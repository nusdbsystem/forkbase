# ForkBase Commandline Console API

## NOTE

Currently, we only support ``VString`` related object operations.

## <a name="get">Get a value</a>

```
  [command] get

    // get the value of a branch head
    get $KEY -b $BRANCH
    // get the value of a version
    get $KEY -v $VERSION
```

## <a name="put">Put a value</a>

```
  [command] put

    // put a value referring a branch
    put $KEY $VALUE -b $BRANCH

    // put a value referring an existing version
    put $KEY $VALUE -v $VERSION
```

## <a name="merge">Merge two values</a>

```
  [command] merge

    // merge target branch to a refering branch
    merge $KEY $VALUE -b $BRANCH -b $BRANCH

    // merge target branch to a refering version
    merge $KEY $VALUE -b $BRANCH -v $VERSION

    // merge two existing versions
    merge $KEY $VALUE -v $VERSION -v $VERSION
```

## <a name="branch">Create a branch</a>

```
  [command] branch

    // create a new branch which points to the head of a branch
    branch $NEW_BRANCH -b $OLD_BRANCH

    // create a new branch which points to an existing version
    branch $NEW_BRANCH -v $OLD_VERSION
```

## <a name="rename">Rename a branch</a>

```
  [command] rename

    // rename an existing branch
    rename $OLD_BRANCH $NEW_BRANCH
```

## <a name="delete">Delete a branch</a>

```
  [command] delete

    // delete an existing branch
    delete $OLD_BRANCH
```

## <a name="list">List keys and branches</a>

```
  [command] list

    // list all keys
    list
    // list all branches of a key
    list $KEY
```

## <a name="head">Get branch head</a>

```
  [command] head

    // get head version of a branch
    head $KEY $BRANCH
```

## <a name="latest">Get latest versions</a>

```
  [command] latest

    // get all latest versions of a key
    latest $KEY
```
