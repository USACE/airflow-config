package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/kelseyhightower/envconfig"
)

func sleep(durString string) {
	d, err := time.ParseDuration(durString)
	if err != nil {
		log.Panicln(err.Error())
	}
	fmt.Printf("Sleeping for %f minutes\n", d.Minutes())
	time.Sleep(d)
}

func printError(err error) {
	if err != nil {
		fmt.Printf(err.Error())
	}
}

func main() {

	// Config stores configuration information stored in environment variables
	type Config struct {
		RepositoryURL string `envconfig:"REPOSITORY_URL"`
		RemoteName    string `envconfig:"REMOTE_NAME" default:"origin"`
		RemoteBranch  string `envconfig:"REMOTE_BRANCH"`
		PullInterval  string `envconfig:"PULL_INTERVAL" default:"2m"`
	}

	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		log.Fatal(err.Error())
	}

	path := "/data"

	_, err = git.PlainClone(path, false, &git.CloneOptions{
		URL:           cfg.RepositoryURL,
		RemoteName:    cfg.RemoteName,
		ReferenceName: plumbing.NewBranchReferenceName(cfg.RemoteBranch),
		SingleBranch:  true,
		Progress:      os.Stdout,
	})
	printError(err)

	for {
		sleep(cfg.PullInterval)

		r, err := git.PlainOpen(path)
		printError(err)

		w, err := r.Worktree()
		printError(err)

		err = w.Pull(&git.PullOptions{
			RemoteName:    cfg.RemoteName,
			ReferenceName: plumbing.NewBranchReferenceName(cfg.RemoteBranch),
			SingleBranch:  true,
		})
		printError(err)

		// Print the latest commit that was just pulled
		ref, err := r.Head()
		printError(err)

		commit, err := r.CommitObject(ref.Hash())
		printError(err)

		fmt.Printf("Current Commit:\n\n%s\n", commit)

	}

}
