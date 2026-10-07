// polyspool persists the unchanged PMXT Redis payload before expensive work.
// WAL v1: gzip(PDW1 + frames); frame = length:u32 crc32:u32 seq:u64 recv_ns:i64 payload.
// CRC covers seq+recv_ns+payload. Integers are big-endian. Seq is LOCAL, not exchange.
package main

import (
 "bufio"
 "compress/gzip"
 "context"
 "crypto/rand"
 "encoding/binary"
 "encoding/hex"
 "encoding/json"
 "errors"
 "fmt"
 "hash/crc32"
 "io"
 "net"
 "os"
 "os/exec"
 "os/signal"
 "path/filepath"
 "strconv"
 "strings"
 "sync"
 "syscall"
 "time"
)
const maxFrame = 32 << 20
var errDisk = errors.New("disk reserve reached; unarchived data is never deleted")
func env(k, fallback string) string { if v:=os.Getenv(k); v!="" {return v};return fallback }
func num(k string, fallback int64) int64 {
 v,e:=strconv.ParseInt(env(k,strconv.FormatInt(fallback,10)),10,64)
 if e!=nil || v<0 {panic("invalid "+k)};return v
}
func freeBytes(path string) uint64 { var s syscall.Statfs_t; if syscall.Statfs(path,&s)!=nil {return 0};return s.Bavail*uint64(s.Bsize) }
func syncDir(path string) error {f,e:=os.Open(path);if e!=nil{return e};defer f.Close();return f.Sync()}
func atomicJSON(path string, v any) error {
 b,e:=json.Marshal(v);if e!=nil{return e}
 f,e:=os.OpenFile(path+".tmp",os.O_CREATE|os.O_TRUNC|os.O_WRONLY,0600);if e!=nil{return e}
 if _,e=f.Write(b);e==nil{e=f.Sync()};ce:=f.Close();if e!=nil{return e};if ce!=nil{return ce}
 if e=os.Rename(path+".tmp",path);e!=nil{return e};return syncDir(filepath.Dir(path))
}
type recorder struct {
 mu sync.Mutex
 root,kind,session,path string
 lock,f *os.File
 z *gzip.Writer
 seq,received,appended,durable uint64
 lastReceive,opened int64
 subscribed,pressure bool
 floor uint64
}
func newRecorder(root,kind string)(*recorder,error){
 for _,p:=range []string{filepath.Join(root,"spool",kind),filepath.Join(root,"state")} {
  if e:=os.MkdirAll(p,0700);e!=nil{return nil,e}
 }
 lock,e:=os.OpenFile(filepath.Join(root,"state",kind+".lock"),os.O_CREATE|os.O_RDWR,0600)
 if e!=nil{return nil,e}
 if e=syscall.Flock(int(lock.Fd()),syscall.LOCK_EX|syscall.LOCK_NB);e!=nil{lock.Close();return nil,e}
 b:=make([]byte,12);if _,e=rand.Read(b);e!=nil{lock.Close();return nil,e}
 r:=&recorder{root:root,kind:kind,session:hex.EncodeToString(b),lock:lock,floor:uint64(num("DISK_RESERVE_BYTES",2<<30))}
 old,_:=os.ReadFile(filepath.Join(root,"state",kind+".json"));var prev map[string]any;_ = json.Unmarshal(old,&prev)
 files,_:=filepath.Glob(filepath.Join(root,"spool",kind,"*.open"))
 // The owning writer holds the lock; the converter never renames live files.
 for _,p:=range files {if e=os.Rename(p,strings.TrimSuffix(p,".open")+".recovered");e!=nil{lock.Close();return nil,e}}
 if len(files)>0 {if e=syncDir(filepath.Join(root,"spool",kind));e!=nil{lock.Close();return nil,e}}
 if e=r.control("process_start",map[string]any{"previous_status":prev,"recovered_segments":len(files),"history_complete":false});e!=nil{lock.Close();return nil,e}
 return r,nil
}
func(r *recorder)openLocked()error{
 if r.f!=nil{return nil};if freeBytes(r.root)<r.floor{return errDisk}
 r.opened=time.Now().UnixNano();r.path=filepath.Join(r.root,"spool",r.kind,fmt.Sprintf("%019d-%s.open",r.opened,r.session))
 f,e:=os.OpenFile(r.path,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600);if e!=nil{return e}
 z,e:=gzip.NewWriterLevel(f,gzip.BestSpeed);if e!=nil{f.Close();return e}
 r.f,r.z=f,z;_,e=z.Write([]byte("PDW1"));return e
}
func(r *recorder)appendLocked(payload []byte,source bool)error{
 if len(payload)>maxFrame{return errors.New("oversized frame")}
 if source {r.received++;r.lastReceive=time.Now().UnixNano();if r.pressure{return errDisk}}
 if e:=r.openLocked();e!=nil{return e}
 r.seq++;h:=make([]byte,24);binary.BigEndian.PutUint32(h,uint32(len(payload)))
 binary.BigEndian.PutUint64(h[8:],r.seq);binary.BigEndian.PutUint64(h[16:],uint64(time.Now().UnixNano()))
 c:=crc32.NewIEEE();_,_=c.Write(h[8:]);_,_=c.Write(payload);binary.BigEndian.PutUint32(h[4:],c.Sum32())
 if _,e:=r.z.Write(h);e!=nil{return e};if _,e:=r.z.Write(payload);e!=nil{return e}
 if source{r.appended++};return nil
}
func(r *recorder)append(payload []byte)error{r.mu.Lock();defer r.mu.Unlock();return r.appendLocked(payload,true)}
func(r *recorder)control(reason string,details map[string]any)error{
 b,_:=json.Marshal(map[string]any{"_polydata_control":true,"reason":reason,"details":details})
 r.mu.Lock();defer r.mu.Unlock();return r.appendLocked(b,false)
}
func(r *recorder)sealLocked()error{
 if r.f==nil{return nil}
 if e:=r.z.Close();e!=nil{return e};if e:=r.f.Sync();e!=nil{return e};if e:=r.f.Close();e!=nil{return e}
 r.f,r.z=nil,nil
 if e:=os.Rename(r.path,strings.TrimSuffix(r.path,".open")+".ready");e!=nil{return e}
 r.durable=r.appended;return syncDir(filepath.Dir(r.path))
}
func(r *recorder)checkpoint()error{
 r.mu.Lock();defer r.mu.Unlock()
 free:=freeBytes(r.root);r.pressure=free<r.floor
 if r.f!=nil {
  if e:=r.z.Flush();e!=nil{return e};if e:=r.f.Sync();e!=nil{return e};r.durable=r.appended
  st,e:=r.f.Stat();if e!=nil{return e}
  if st.Size()>=num("WAL_SEGMENT_BYTES",32<<20)||time.Now().UnixNano()-r.opened>=num("WAL_ROTATE_SECONDS",300)*int64(time.Second){if e=r.sealLocked();e!=nil{return e}}
 }
 return atomicJSON(filepath.Join(r.root,"state",r.kind+".json"),map[string]any{
  "time_ns":time.Now().UnixNano(),"session":r.session,"source":r.kind,"received":r.received,"appended":r.appended,
  "fsynced":r.durable,"last_receive_ns":r.lastReceive,"subscribed":r.subscribed,"free_bytes":free,"disk_pressure":r.pressure})
}
func(r *recorder)close()error{r.mu.Lock();defer r.mu.Unlock();defer r.lock.Close();return r.sealLocked()}
func(r *recorder)connected(v bool){r.mu.Lock();r.subscribed=v;r.mu.Unlock()}
func(r *recorder)last()int64{r.mu.Lock();defer r.mu.Unlock();return r.lastReceive}
func(r *recorder)maintenance(ctx context.Context,failed chan<- error,done chan<- struct{}){
 defer close(done);t:=time.NewTicker(time.Second);defer t.Stop()
 for{select{case<-ctx.Done():return;case<-t.C:if e:=r.checkpoint();e!=nil{failed<-e;return}}}
}
// RESP2 decoder with bounded line length, bulk payload and nesting.
func resp(br *bufio.Reader,depth int)(any,error){
 if depth>4{return nil,errors.New("RESP nesting limit")}
 line,e:=br.ReadSlice('\n');if e!=nil{return nil,e}
 if len(line)<3||len(line)>1024||!strings.HasSuffix(string(line),"\r\n"){return nil,errors.New("bad RESP line")}
 body:=string(line[1:len(line)-2]);kind:=line[0]
 switch kind{
 case '+':return body,nil
 case '-':return nil,fmt.Errorf("Redis: %s",body)
 case ':':return strconv.ParseInt(body,10,64)
 case '$':
  n,e:=strconv.Atoi(body);if e!=nil||n < -1||n>maxFrame{return nil,errors.New("bad bulk length")};if n== -1{return nil,nil}
  b:=make([]byte,n+2);if _,e=io.ReadFull(br,b);e!=nil{return nil,e};if string(b[n:])!="\r\n"{return nil,errors.New("bad bulk terminator")};return b[:n],nil
 case '*':
  n,e:=strconv.Atoi(body);if e!=nil||n<0||n>64{return nil,errors.New("bad array length")};a:=make([]any,n)
  for i:=range a{a[i],e=resp(br,depth+1);if e!=nil{return nil,e}};return a,nil
 };return nil,errors.New("unknown RESP type")
}
func command(w io.Writer,parts ...string)error{
 var b strings.Builder;fmt.Fprintf(&b,"*%d\r\n",len(parts));for _,p:=range parts{fmt.Fprintf(&b,"$%d\r\n%s\r\n",len(p),p)};_,e:=io.WriteString(w,b.String());return e
}
func bulkString(v any)string{if b,ok:=v.([]byte);ok{return string(b)};if s,ok:=v.(string);ok{return s};return ""}
type writeFailure struct{error}
func subscribe(ctx context.Context,r *recorder)error{
 c,e:=net.DialTimeout("tcp",env("REDIS_ADDR","redis:6379"),10*time.Second);if e!=nil{return e};defer c.Close()
 if e=command(c,"SUBSCRIBE",env("REDIS_CHANNEL","polymarket:events"));e!=nil{return e}
 done:=make(chan struct{});defer close(done)
 go func(){t:=time.NewTicker(15*time.Second);defer t.Stop();for{select{
  case<-ctx.Done():c.Close();return
  case<-done:return
  case<-t.C:_=c.SetWriteDeadline(time.Now().Add(5*time.Second));if command(c,"PING")!=nil{c.Close();return}
 }}}()
 br:=bufio.NewReaderSize(c,64<<10)
 for{
  _=c.SetReadDeadline(time.Now().Add(45*time.Second));v,e:=resp(br,0);if e!=nil{return e}
  a,ok:=v.([]any);if !ok||len(a)<2{return errors.New("unexpected Redis response")}
  switch bulkString(a[0]){
  case "subscribe":r.connected(true);if e=r.control("subscriber_connected",map[string]any{"state_recovered":false});e!=nil{return writeFailure{e}};if e=r.checkpoint();e!=nil{return writeFailure{e}}
  case "message":
   if len(a)!=3{return errors.New("malformed Redis message")};p,ok:=a[2].([]byte);if !ok{return errors.New("non-bulk Redis message")}
   if e=r.append(p);e!=nil{if errors.Is(e,errDisk){return e};return writeFailure{e}}
  case "pong":
  default:return errors.New("unexpected pubsub response kind")
  }
 }
}
func receive(ctx context.Context,r *recorder)error{
 delay:=time.Second
 for ctx.Err()==nil{
  started:=time.Now();e:=subscribe(ctx,r);r.connected(false)
  if ctx.Err()!=nil{return nil}
  var wf writeFailure;if errors.As(e,&wf){return e}
  if time.Since(started)>time.Minute{delay=time.Second}
  reason:="subscriber_disconnected";if errors.Is(e,errDisk){reason="disk_pressure"}
  if ce:=r.control(reason,map[string]any{"error":fmt.Sprint(e),"needs_manual_backfill":true,"lost_messages":nil,"last_receive_ns":r.last()});ce!=nil{return ce}
  if ce:=r.checkpoint();ce!=nil{return ce};fmt.Fprintln(os.Stderr,reason)
  select{case<-ctx.Done():return nil;case<-time.After(delay):}
  if delay<30*time.Second{delay*=2}
 };return nil
}
func tap(ctx context.Context,r *recorder,args []string)error{
 if len(args)==0{return errors.New("tap needs a command")}
 cmd:=exec.Command(args[0],args[1:]...);cmd.SysProcAttr=&syscall.SysProcAttr{Setpgid:true}
 rd,wr,e:=os.Pipe();if e!=nil{return e};defer rd.Close();cmd.Stdout,cmd.Stderr=wr,wr
 if e=cmd.Start();e!=nil{wr.Close();return e};wr.Close()
 done:=make(chan struct{});defer close(done)
 go func(){select{case<-ctx.Done():
  _=syscall.Kill(-cmd.Process.Pid,syscall.SIGTERM)
  select{case<-done:return;case<-time.After(20*time.Second):_=syscall.Kill(-cmd.Process.Pid,syscall.SIGKILL)}
 case<-done:}}()
 scan:=bufio.NewScanner(rd);scan.Buffer(make([]byte,64<<10),maxFrame)
 for scan.Scan(){line:=append([]byte(nil),scan.Bytes()...)
  if e=r.append(line);e!=nil{_=syscall.Kill(-cmd.Process.Pid,syscall.SIGKILL);_=cmd.Wait();return e}
  fmt.Println(string(line))
 }
 if scan.Err()!=nil{_=syscall.Kill(-cmd.Process.Pid,syscall.SIGKILL)}
 waitErr:=cmd.Wait();if scan.Err()!=nil{return scan.Err()}
 if e=r.control("upstream_exit",map[string]any{"error":fmt.Sprint(waitErr),"needs_manual_backfill":true});e!=nil{return e}
 return waitErr
}
func run()(retErr error){
 if len(os.Args)<2{return errors.New("usage: polyspool receive | tap command... | health")}
 root:=env("DATA_DIR","/data")
 if os.Args[1]=="health"{
  b,e:=os.ReadFile(filepath.Join(root,"state","raw.json"));if e!=nil{return e}
  var s struct{Time int64 `json:"time_ns"`;Subscribed bool `json:"subscribed"`;Pressure bool `json:"disk_pressure"`}
  if e=json.Unmarshal(b,&s);e!=nil{return e}
  if time.Now().UnixNano()-s.Time>30*int64(time.Second)||!s.Subscribed||s.Pressure{return errors.New("receiver not ready")};return nil
 }
 kind:="raw";if os.Args[1]=="tap"{kind="upstream"}else if os.Args[1]!="receive"{return errors.New("invalid mode")}
 ctx,cancel:=signal.NotifyContext(context.Background(),os.Interrupt,syscall.SIGTERM);defer cancel()
 r,e:=newRecorder(root,kind);if e!=nil{return e}
 defer func(){if ce:=r.close();retErr==nil{retErr=ce}}()
 failed,maintDone:=make(chan error,1),make(chan struct{});go r.maintenance(ctx,failed,maintDone)
 result:=make(chan error,1)
 go func(){if kind=="upstream"{result<-tap(ctx,r,os.Args[2:])}else{result<-receive(ctx,r)}}()
 select{case e=<-result:cancel();case e=<-failed:cancel();<-result}
 <-maintDone
 return e
}
func main(){if e:=run();e!=nil{fmt.Fprintln(os.Stderr,e);os.Exit(1)}}
