package main

import "fmt"

type Partition struct {
	partitionID uint16
	log         *AppendOnlyLog
}

func (p *Partition) init(id uint16, dataDir string, topicID uint16) error {
	p.partitionID = id
	path := fmt.Sprintf("%s/topic-%d/partition-%d", dataDir, topicID, id)
	log, err := OpenAppendOnlyLog(path)
	if err != nil {
		return err
	}
	p.log = log
	return nil
}
