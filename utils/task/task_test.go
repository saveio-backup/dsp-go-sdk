package task

import (
	"fmt"
	"testing"
)

func TestTotalJitterDelay(t *testing.T) {
	sum := uint64(0)
	for i := 1; i <= 50; i++ {
		sum += GetJitterDelay(i, 30)
	}
	fmt.Printf("total sec %d, hour %v\n", sum, sum/3600)
}

func TestGetDecryptedFilePath1(t *testing.T) {
	type args struct {
		filePath string
		fileName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				filePath: "~/test.txt",
				fileName: "test.txt",
			},
			want: "~/test.txt-decrypted",
		},
		{
			args: args{
				filePath: "~/test.txt",
				fileName: "test",
			},
			want: "~/test.txt-decrypted",
		},
		{
			args: args{
				filePath: "~/test (1).txt",
				fileName: "test",
			},
			want: "~/test (1).txt-decrypted",
		},
		{
			args: args{
				filePath: "~/test (1).txt",
				fileName: "test.txt",
			},
			want: "~/test (1).txt-decrypted",
		},
		{
			args: args{
				filePath: "~/test.ept",
				fileName: "test.txt",
			},
			want: "~/test.txt",
		},
		{
			args: args{
				filePath: "~/test.ept",
				fileName: "test",
			},
			want: "~/test",
		},
		{
			args: args{
				filePath: "~/test (1).ept",
				fileName: "test.txt",
			},
			want: "~/test (1).txt",
		},
		{
			args: args{
				filePath: "~/test (1).ept",
				fileName: "test",
			},
			want: "~/test (1)",
		},
		// why
		{
			args: args{
				filePath: "a/b/c/d.ept/.DS_Store.ept",
				fileName: ".DS_Store.ept",
			},
			want: "a/b/c/d.ept/.DS_Store.ept",
		},
		{
			args: args{
				filePath: "a/b/c/d.ept/a.DS_Store.ept",
				fileName: "a.DS_Store.ept",
			},
			want: "a/b/c/d.ept/a.DS_Store.ept",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDecryptedFilePath(tt.args.filePath, tt.args.fileName); got != tt.want {
				t.Errorf("GetDecryptedFilePath() = %v, want %v, args: %v", got, tt.want, tt.args)
			}
		})
	}
}
