# Important GIT commands

* Command to get the updated code from a branch. For example let's assume we want to update our local branch **fbref_match** with the remote branch. Then the following command should be enough
```
git pull origin fbref_match
```

* Command to see the currently, checkout branch
```
git branch 
```

* Command to see all the existing branches
```
git branch -a
```

* Command to see all the remote branches
```
git branch -r
```

* Command to checkout out a particular branch e.g **fbref_match**
```
git checkout fbref_match
``` 

* Command to check the current status of the checkout branch
```
git status
```

* Command to stage modified files
```
git add /path_to_file2 /path_to_file2 
git add . 
```

* Command to commit changes
```
git commit -m "COMMIT_TYPE: COMMIT_MESSAGE"
```

* Command to push to a particular branch
```
git push origin HEAD:fbref_match
```