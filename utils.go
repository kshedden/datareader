package datareader

type Statfilereader interface {
	ColumnNames() []string
	ColumnTypes() []int
	Read(int) ([]*Series, error)
}
