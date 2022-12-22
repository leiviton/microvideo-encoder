package services

import (
	"encoder/application/repositories"
	"encoder/domain"
	"errors"
	"os"
	"strconv"
)

type JobService struct {
	Job           *domain.Job
	JobRepository repositories.JobRepository
	VideoService  VideoService
}

func (j *JobService) Start() error {
	//start download
	err := j.chanceJobStatus("DOWNLOADING")
	if err != nil {
		return j.failJob(err)
	}

	err = j.VideoService.Download(os.Getenv("inputBucketName"))
	if err != nil {
		return j.failJob(err)
	}
	//finish download
	//start fragment
	err = j.chanceJobStatus("FRAGMENTING")
	if err != nil {
		return j.failJob(err)
	}

	err = j.VideoService.Fragment()
	if err != nil {
		return j.failJob(err)
	}
	//finish fragment
	//start encoder
	err = j.chanceJobStatus("ENCODING")
	if err != nil {
		return j.failJob(err)
	}

	err = j.VideoService.Encode()
	if err != nil {
		return j.failJob(err)
	}
	//finish encoder
	//start upload
	err = j.chanceJobStatus("UPLOADING")
	if err != nil {
		return j.failJob(err)
	}

	err = j.performUpload()
	if err != nil {
		return j.failJob(err)
	}
	//finish upload
	//start finish
	err = j.chanceJobStatus("FINISHING")
	if err != nil {
		return j.failJob(err)
	}

	err = j.VideoService.Finish()
	if err != nil {
		return j.failJob(err)
	}
	//finish FINISHING
	//start completed
	err = j.chanceJobStatus("COMPLETED")
	if err != nil {
		return j.failJob(err)
	}
	//END
	return nil
}

func (j *JobService) performUpload() error {
	err := j.chanceJobStatus("UPLOADING")
	if err != nil {
		return j.failJob(err)
	}

	videoUpload := NewVideoUpload()
	videoUpload.OutPutBucket = os.Getenv("outputBucketName")
	videoUpload.VideoPath = os.Getenv("localStoragePath") + "/" + j.VideoService.Video.ID
	concurrency, _ := strconv.Atoi(os.Getenv("concurrency_upload"))
	doneUpload := make(chan string)

	go videoUpload.ProcessUpload(concurrency, doneUpload)
	var uploadResult string
	uploadResult = <-doneUpload

	if uploadResult != "upload completed" {
		return j.failJob(errors.New(uploadResult))
	}

	return err
}
func (j *JobService) chanceJobStatus(status string) error {

	var err error

	j.Job.Status = status
	j.Job, err = j.JobRepository.Update(j.Job)

	if err != nil {
		return j.failJob(err)
	}

	return nil
}

func (j *JobService) failJob(error error) error {

	j.Job.Status = "FAILED"
	j.Job.Error = error.Error()

	_, err := j.JobRepository.Update(j.Job)
	if error != nil {
		return err
	}

	return error
}
