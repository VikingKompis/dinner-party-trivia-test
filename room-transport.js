/* HTTPS room transport. No WebRTC, same-network requirement or third-party scripts. */
(()=>{
 const API=location.hostname==='vikingkompis.github.io'?'https://dinner-party-trivia-erik.erikbroberg.chatgpt.site/api':location.origin+'/api';
 const hex=n=>Array.from(crypto.getRandomValues(new Uint8Array(n)),x=>x.toString(16).padStart(2,'0')).join('');
 class Emitter{constructor(){this.handlers={}}on(name,fn){(this.handlers[name]??=[]).push(fn);return this}emit(name,...args){for(const fn of this.handlers[name]||[])fn(...args)}}
 class Link extends Emitter{
  constructor(owner,remote){super();this.owner=owner;this.peer=remote;this.open=false;this.queue=[];this.sending=false}
  send(data){if(!this.open)throw Error('Connection is not open');this.queue.push({id:crypto.randomUUID(),to:this.peer,data});this.flush()}
  async flush(){if(this.sending)return;this.sending=true;while(this.queue.length&&!this.owner.destroyed){try{await this.owner.request('send',{method:'POST',body:this.queue[0]});this.queue.shift()}catch(e){if(e.status&&e.status<500&&e.status!==429){this.emit('error',e);this.queue.shift()}else await this.owner.delay(700)}}this.sending=false}
  close(){if(this.open){this.open=false;this.emit('close')}}
 }
 class RoomPeer extends Emitter{
  constructor(id){super();this.destroyed=false;this.disconnected=false;this.links=new Map();this.cursor=0;this.waiters=[];this.tv=!!id;this.id=hex(16);this.failures=0;queueMicrotask(()=>this.init())}
  async init(){try{if(this.tv){this.token=hex(32);const r=await this.request('',{method:'POST',body:{token:this.token},create:true});this.room=r.code;this.id='dpt-'+r.code;this.emit('open',this.id);this.poll()}else this.emit('open',this.id)}catch(e){this.emit('error',e)}}
  connect(remote){const link=new Link(this,'tv');this.links.set('tv',link);this.room=remote.replace(/^dpt-/,'');let session;try{session=JSON.parse(localStorage.getItem('dpt_room_'+this.room))}catch{}if(!session){session={id:hex(16),token:hex(32)};try{localStorage.setItem('dpt_room_'+this.room,JSON.stringify(session))}catch{}}this.id=session.id;this.token=session.token;this.cursor=session.cursor||0;this.session=session;
   this.request('join',{method:'POST',body:{id:this.id}}).then(()=>{if(this.destroyed)return;link.open=true;link.emit('open');this.poll()}).catch(e=>this.emit('error',e));return link}
  async request(action,{method='GET',body,create=false}={}){const u=create?API+'/rooms':API+'/rooms/'+this.room+'/'+action;const r=await fetch(u,{method,headers:{'Content-Type':'application/json',...(create?{}:{Authorization:'Bearer '+this.token})},body:body?JSON.stringify(body):undefined,cache:'no-store',signal:AbortSignal.timeout(10000)});let d;try{d=await r.json()}catch{throw Error('The game service did not respond. Please try again.')}if(!r.ok){const e=Error(d.error||'Connection failed');e.status=r.status;e.type='network';throw e}return d}
  async poll(){while(!this.destroyed){try{const r=await this.request('events?after='+this.cursor);if(this.destroyed)return;this.failures=0;
    for(const item of r.events){let link=this.links.get(item.from);if(!link||!link.open){link=new Link(this,item.from);link.open=true;this.links.set(item.from,link);if(this.tv)this.emit('connection',link)}link.emit('data',item.data);this.cursor=item.seq}
    if(this.session){this.session.cursor=this.cursor;try{localStorage.setItem('dpt_room_'+this.room,JSON.stringify(this.session))}catch{}}
    if(this.tv&&r.online){for(const [id,link] of this.links)if(!r.online.includes(id))link.close()}
   }catch(e){if(this.destroyed)return;if(e.status===404||e.status===410||e.status===403){this.emit('error',e);break}this.failures++;if(this.failures===3)this.emit('disconnected')}
   await this.delay(this.failures?1500:450)
  }}
  delay(ms){return new Promise(resolve=>{const w={resolve,timer:0};w.timer=setTimeout(()=>{this.waiters=this.waiters.filter(x=>x!==w);resolve()},ms);this.waiters.push(w)})}
  reconnect(){/* Polling and queued messages retry automatically. */}
  destroy(){this.destroyed=true;for(const link of this.links.values())link.close();for(const w of this.waiters){clearTimeout(w.timer);w.resolve()}this.waiters=[]}
 }
 window.RoomPeer=RoomPeer;
})();
