use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Select {
    db: String,
}

impl Select {
  pub fn new(db: impl ToString) -> Select {
    Select {
      db: db.to_string(),
    }
  }

  pub fn db(&self) -> &str {
    &self.db
  }

  pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
    let db = parse.next_string()?;

    Ok(Select { db })
  }

  // Nothing here yet. Just NO-OP for now.
  #[instrument(skip(self, db, dst))]
  pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
    let response = Frame::Simple("OK".to_string());

    dst.write_frame(&response).await?;

    Ok(())
  }

  pub(crate) fn into_frame(self) -> Frame {
    let mut frame = Frame::array();
    frame.push_bulk(Bytes::from("select".as_bytes()));
    frame.push_bulk(Bytes::from(self.db.into_bytes()));
    frame
  }
}