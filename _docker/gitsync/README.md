## gitsync-container-volume

A simple container written in pure go to keep a directory up-to-date with a remote git repository.

Does the equivalent of a `git clone`, followed by repeated `git pull` at a specified interval into the directory `/data` using the [go-git](https://github.com/go-git/go-git) project.

This is useful as a sidecar container to share files stored in a git repository with other containers via shared volume.

Configuration is accomplished using these environment variables:

| Variable       | Required | Default  | Description                                                                                                                |
| -------------- | -------- | -------- | -------------------------------------------------------------------------------------------------------------------------- |
| REPOSITORY_URL | X        |          | Repository root URL. example: `https://github.com/USACE/gitsync-container-volume`                                          |
| REMOTE_NAME    |          | "origin" | Remote name. example: `origin`                                                                                    |
| REMOTE_BRANCH  | X        |          | Branch to clone, pull. example: `develop`                                                                                  |
| PULL_INTERVAL  |          | "2m"     | Interval between `git pull`s after `git clone`; Supports [Go duration strings](https://golang.org/pkg/time/#ParseDuration) |
