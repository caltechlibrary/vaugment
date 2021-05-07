# tracking changes over time to archivesspace records

1. set up baseline records
2. run database comparisons nightly
3. parse changes
4. download new files / delete files for deleted records
5. commit to git

## potential problems

- filenames ought to be human-readable for viewing git diffs

## TODO

- clean up unused files
- filter changes by set of users in `last_modified_by`
