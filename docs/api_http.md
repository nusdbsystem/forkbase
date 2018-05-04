# UStore Http Service RESTful API

## NOTE

Currently, we only support ``VString`` related object operations.

## <a name="get">Get a value</a>

```
  [public] /get

    // get the value of a branch head
    POST -d "key=XXX&branch=XXX"

    // get the value of a version
    POST -d "key=XXX&version=XXX"
```

## <a name="put">Put a value</a>

```
  [public] /put

    // put a value referring a branch
    POST -d "key=XXX&branch=XXX&value=XXX"

    // put a value referring an existing version
    POST -d "key=XXX&version=XXX&value=XXX"
```

## <a name="merge">Merge two values</a>

```
  [public] /merge

    // merge target branch to a refering branch
    POST -d "key=XXX&tgt_branch=XXX&ref_branch=XXX&value=XXX"

    // merge target branch to a refering version
    POST -d "key=XXX&tgt_branch=XXX&ref_version1=XXX&value=XXX"

    // merge two existing versions
    POST -d "key=XXX&ref_version1=XXX&ref_version2=XXX&value=XXX"
```

## <a name="branch">Create a branch</a>

```
  [public] /branch

    // create a new branch which points to the head of a branch
    POST -d "key=XXX&old_branch=XXX&new_branch=XXX"

    // create a new branch which points to an existing version
    POST -d "key=XXX&version=XXX&new_branch=XXX"
```

## <a name="rename">Rename a branch</a>

```
  [public] /rename

    // rename an existing branch
    POST -d "key=XXX&old_branch=XXX&new_branch=XXX"
```

## <a name="delete">Delete a branch</a>

```
  [public] /delete

    // delete an existing branch
    POST -d "key=XXX&branch=XXX"
```

## <a name="list">List keys and branches</a>

```
  [public] /list

    // list all keys
    GET
    // list all branches of a key
    POST -d "key=XXX"
```

## <a name="head">Get branch head</a>

```
  [public] /head

    // get head version of a branch
    POST -d "key=XXX&branch=XXX"
```

## <a name="latest">Get latest versions</a>

```
  [public] /latest

    // get all latest versions of a key
    POST -d "key=XXX"
```
