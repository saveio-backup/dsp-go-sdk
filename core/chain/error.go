package chain

func (c *Chain) FormatError(err error) error {
	return c.client.FormatError(err)
}
