package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/go-ini/ini"
	"github.com/jawher/mow.cli"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/diff"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

// Describe a project segment and its members and resources
// ProjectSegment can be any logical piece of a project
type ProjectSegment struct {
	// Name of the segment
	Name string `ini:"-"`
	// Repository to submit patches
	Repository string
	// URL of the chat service
	Chat string
	// URL of the mailing list
	MailList string
	// URL of the issue tracker
	IssueTracker string
	// Comma separated list of project members who are responsible for this Segment
	Chiefs []string
	// Comma separated list of project members who are responsible only for code reviews in this Segment
	Reviewers []string
	// List of regexps to specify which file to include in this Segment
	FilePatterns []string
	// List of regexps to specify what patch content should be included in this Segment
	ContentPatterns []string
	// List of regexps to exclude files matched by FilePatterns regex
	FileExcludePatterns []string
	// List of regexps to exclude patch content matched by `ContentPatterns`
	ContentExcludePatterns []string
	// If a changeset affects multiple segments, priority can describe the order of segments listed
	Priority int
	// Comma separated list of segment's topics
	Topics []string
}

type ProjectSegments map[string]*ProjectSegment

type Config struct {
	Segments ProjectSegments
}

type orderedSegmentList []*ProjectSegment

func (o orderedSegmentList) Len() int           { return len(o) }
func (o orderedSegmentList) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
func (o orderedSegmentList) Less(i, j int) bool { return o[i].Priority > o[j].Priority }

// entry point
func main() {
	app := cli.App("chiefr", "Distributed source code maintennance toolkit")
	mf := app.StringOpt("m maintainers-file", ".maintainers.ini", "Maintainers configuration file")
	var config *Config

	app.Before = func() {
		// load config
		var err error
		config, err = initMaintainers(*mf)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if config.Segments == nil {
			fmt.Println("Error: empty maintainers file")
			os.Exit(2)
		}
		if len(config.Segments) == 0 {
			fmt.Println("Warning! No project segments defined.")
		}
	}

	app.Command("add", "Add new segment", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			fmt.Println("not implemented")
		}
	})
	app.Command("list", "List maintainers", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			for _, segment := range config.Segments {
				fmt.Printf("%s\n\n", segment.String())
			}
		}
	})
	app.Command("submit", "Submit patches to maintainers", func(cmd *cli.Cmd) {
		ref := cmd.StringArg("REVISION", "", "Base git revision of the patch")
		cmd.Action = func() {
			err := submit(config, "./", *ref)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(3)
			}
		}
	})

	app.Action = func() {
		app.PrintHelp()
	}
	app.Run(os.Args)
}

func (s *ProjectSegment) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("[%s]\n", s.Name))
	buf.WriteString(fmt.Sprintf(" Chiefs: %s\n", strings.Join(s.Chiefs, ", ")))
	buf.WriteString(fmt.Sprintf(" Priority: %d\n", s.Priority))
	if len(s.Topics) != 0 {
		buf.WriteString(fmt.Sprintf(" Topics: %s\n", strings.Join(s.Topics, ", ")))
	}
	if len(s.Reviewers) != 0 {
		buf.WriteString(fmt.Sprintf(" Reviewers: %s\n", strings.Join(s.Reviewers, ", ")))
	}
	if s.Repository != "" {
		buf.WriteString(fmt.Sprintf(" Repository: %s\n", s.Repository))
	}
	if s.IssueTracker != "" {
		buf.WriteString(fmt.Sprintf(" Issue tracker: %s\n", s.IssueTracker))
	}
	if s.MailList != "" {
		buf.WriteString(fmt.Sprintf(" Mailing list: %s\n", s.MailList))
	}
	if s.Chat != "" {
		buf.WriteString(fmt.Sprintf(" Chat: %s\n", s.Chat))
	}
	if len(s.Reviewers) != 0 {
		buf.WriteString(fmt.Sprintf(" Reviewers: %s\n", strings.Join(s.Reviewers, ", ")))
	}
	if len(s.FilePatterns) != 0 {
		buf.WriteString(fmt.Sprintf(" File patterns: %s\n", strings.Join(s.FilePatterns, ", ")))
	}
	if len(s.ContentPatterns) != 0 {
		buf.WriteString(fmt.Sprintf(" Content patterns: %s\n", strings.Join(s.ContentPatterns, ", ")))
	}
	if len(s.FileExcludePatterns) != 0 {
		buf.WriteString(fmt.Sprintf(" File exclude patterns: %s\n", strings.Join(s.FileExcludePatterns, ", ")))
	}
	if len(s.ContentExcludePatterns) != 0 {
		buf.WriteString(fmt.Sprintf(" Content exclude patterns: %s\n", strings.Join(s.ContentExcludePatterns, ", ")))
	}
	return buf.String()
}

func (s *ProjectSegment) IsConcerned(p diff.FilePatch) bool {
	from, to := p.Files()
	// deletion
	if to == nil {
		to = from
	}
	path := to.Path()
	// file name check
	for _, fp := range s.FilePatterns {
		if match, err := regexp.MatchString(fp, path); !match || err != nil {
			continue
		}
		excluded := false
		for _, fep := range s.FileExcludePatterns {
			if match, err := regexp.MatchString(fep, path); match && err == nil {
				excluded = true
				break
			}
		}
		if !excluded {
			return true
		}
	}
	// TODO sophisticated content matching
	var buffer bytes.Buffer
	for _, chunk := range p.Chunks() {
		// chunk.Type() -> 0: Equal, 1: Add, 2: Delete
		buffer.WriteString(chunk.Content())

	}
	diffContent := buffer.String()
	// content match
	for _, cp := range s.ContentPatterns {
		if match, err := regexp.MatchString(cp, diffContent); !match || err != nil {
			continue
		}
		excluded := false
		for _, cep := range s.ContentExcludePatterns {
			if match, err := regexp.MatchString(cep, diffContent); match && err == nil {
				excluded = true
				break
			}
		}
		if !excluded {
			return true
		}
	}
	return false
}

func initMaintainers(maintainersFileName string) (*Config, error) {
	cfg, err := ini.Load(maintainersFileName)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize maintainers: %s", err.Error())
	}
	c := &Config{Segments: ProjectSegments{}}
	for _, s := range cfg.Sections() {
		if s.Name() == "DEFAULT" {
			continue
		}
		ps := &ProjectSegment{Name: s.Name()}
		err := s.MapTo(ps)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse config section '%s': %s", s.Name(), err)
		}
		if len(ps.Chiefs) == 0 {
			return nil, fmt.Errorf("Invalid config section '%s': missing 'Chiefs' property", s.Name())
		}
		c.Segments[s.Name()] = ps
	}
	return c, nil
}

func submit(c *Config, repoPath, revision string) error {
	segments, err := getPatchSegments(c, repoPath, revision)
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		return fmt.Errorf("No matching segments found for this patch")
	}
	os := make(orderedSegmentList, 0, len(segments))
	for _, s := range segments {
		os = append(os, s)
	}
	sort.Sort(os)

	fmt.Println("Please submit your patch to one of the following repositories:\n")
	for i, s := range os {
		new := true
		for _, s2 := range os[:i] {
			if s2.Repository == s.Repository {
				new = false
				break
			}
		}
		if new {
			fmt.Printf(" - %s\n", s.Repository)
		}
	}
	fmt.Println("")
	return nil
}

func getPatchSegments(c *Config, repoPath, revision string) (ProjectSegments, error) {
	repo, err := git.PlainOpen(repoPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to open git repository:", err.Error())
	}
	head, err := repo.Head()
	if err != nil {
		return nil, fmt.Errorf("Failed to get HEAD of repository: %s", err.Error())
	}
	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, fmt.Errorf("Failed to get HEAD commit: %s", err.Error())
	}
	firstCommit, err := getCommitByRev(repo, revision)
	if err != nil {
		return nil, err
	}
	patch, err := firstCommit.Patch(headCommit)
	if err != nil {
		return nil, fmt.Errorf("Failed to create patch: %s", err.Error())
	}
	relatedSegments := ProjectSegments{}
	for _, p := range patch.FilePatches() {
		for sName, s := range c.Segments {
			if s.IsConcerned(p) {
				relatedSegments[sName] = s
			}
		}
	}
	return relatedSegments, nil
}

func getCommitByRev(repo *git.Repository, revision string) (*object.Commit, error) {
	head, err := repo.Head()
	if err != nil {
		return nil, fmt.Errorf("Failed to get HEAD of repository: %s", err.Error())
	}
	cIter, err := repo.Log(&git.LogOptions{From: head.Hash()})
	if err != nil {
		return nil, fmt.Errorf("Failed to get history of commit range %v..%s: %s", head, revision, err.Error())
	}
	var commit *object.Commit
	err = cIter.ForEach(func(c *object.Commit) error {
		if strings.HasPrefix(c.Hash.String(), revision) {
			commit = c
			return fmt.Errorf("stop")
		}
		return nil
	})
	var rev *plumbing.Hash
	if commit == nil {
		rev, err = repo.ResolveRevision(plumbing.Revision(revision))
		if err != nil {
			rev, err = repo.ResolveRevision(plumbing.Revision("refs/heads/" + revision))
			if err != nil {
				return nil, fmt.Errorf("Failed to resolve revision '%s'", revision)
			}
		}
		commit, err = repo.CommitObject(*rev)
		if err != nil {
			return nil, fmt.Errorf("Failed to resolve revision '%s'", revision)
		}
	}
	return commit, nil
}
