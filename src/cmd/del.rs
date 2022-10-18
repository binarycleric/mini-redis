use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Del {
    key: String,
}

impl Del {
    pub fn new(key: impl ToString) -> Del {
        Del {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let key = parse.next_string()?;

        Ok(Del { key })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.delete(self.key);

        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
      let mut frame = Frame::array();
      frame.push_bulk(Bytes::from("del".as_bytes()));
      frame.push_bulk(Bytes::from(self.key.into_bytes()));
      frame
    }
}
