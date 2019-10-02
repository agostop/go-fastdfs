package fastdfs

import (
	"log"
	"os"
)

type Logger struct {
	Info *log.Logger
	Warn *log.Logger
	Error *log.Logger
}

func NewLogger() *Logger {

	//errorFile, e := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	//if e != nil {
	//	fmt.Println("open log file failed. ", e)
	//}

	return &Logger{
		log.New(os.Stdout, "Info:", log.Ldate|log.Ltime|log.Lshortfile),
		log.New(os.Stdout, "Warn:", log.Ldate|log.Ltime|log.Lshortfile),
		log.New(os.Stderr, "Error:", log.Ldate|log.Ltime|log.Lshortfile),
	}

}
