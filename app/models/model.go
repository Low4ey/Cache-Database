package models

type NodeInfo struct {
	Role             string
	MasterReplid     string
	MasterReplOffset int
	MasterHost       string
	MasterPort       string
	NodePort         int
	Dir              string
	Rdbfilename      string
}
